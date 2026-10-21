
//=====================================================================
// gateway.js — 浏览器 WebSocket → UDP/KCP → Docker kfs 帧同步服务器 网关
//
// 架构：
//   浏览器(WebSocket)  ⇄  gateway.js(本机 Node)  ⇄  Docker 内的 kfs 服务器(UDP)
//
// 本次修改：打通"重连链路"
//   1) KFS_CMD 新增 REJOIN_SYNC=9 / PLAYER_REJOIN=10（与 C 端枚举一致）
//   2) tick() 收包循环不再只放行 BROADCAST，额外识别 9/10 两帧并转发浏览器
//   3) JOIN 扩展应答(21B)：把 frame_interval_ms / room_seed / server_time_ms 透传给浏览器
//   （C 端配套：解注释 kfs_srv_add_player_c 里"新增3/新增4"，详见配套修改说明）
//=====================================================================
'use strict';
const dgram = require('dgram');
const fs = require('fs');
const path = require('path');
const { execFile, execFileSync } = require('child_process');
const { WebSocketServer } = require('ws');
const ikcp = require('./ikcp.js');
//---------------------------------------------------------------------
// 协议常量（与 kfs_src/common/frame.h 一致）
//---------------------------------------------------------------------
const KFS_HANDSHAKE_MAGIC = 0x4B464346;          // "KFCF"
const KFS_HS_LEN2 = 16;
const KFS_FRAME2_HEADER_SIZE = 7;
const KFS_CMD = {
  INPUT: 1, BROADCAST: 2, JOIN: 3, LEAVE: 4,
  HEARTBEAT: 5, STATE: 6, READY: 7,
  REJOIN_SYNC: 9,     // 新增：服务器给重连玩家下发的锚点同步包
  PLAYER_REJOIN: 10,  // 新增：通知房内其他玩家"某 conv 已重连上线"
};
const FNV_OFFSET_BASIS = 2166136261;
const FNV_PRIME = 16777619;
const envServerHost = process.env.KFS_SERVER_HOST || '127.0.0.1';
const envServerFamily = process.env.KFS_SERVER_FAMILY || 'udp4';
function readConfigUdpPort() {
  const candidates = [
    path.join(__dirname, '..', 'lua_test', 'config'),
    path.join(__dirname, 'lua_test', 'config'),
  ];
  for (const p of candidates) {
    try {
      const m = fs.readFileSync(p, 'utf8').match(/^\s*udp_port\s*=\s*"?(\d+)/m);
      if (m) return parseInt(m[1], 10);
    } catch (e) { /* 试下一个位置 */ }
  }
  return 0;
}
const cfg = {
  serverHost: envServerHost || '127.0.0.1',
  serverFamily: envServerFamily || 'udp4',
  serverPort: parseInt(process.env.KFS_SERVER_PORT || String(readConfigUdpPort() || 10500), 10),
  wsPort: parseInt(process.env.WS_PORT || '8081', 10),
  updateMs: parseInt(process.env.KFS_UPDATE_MS || '5', 10),
  joinTimeoutMs: parseInt(process.env.KFS_JOIN_TIMEOUT_MS || '5000', 10),
  dockerContainer: process.env.KFS_DOCKER_CONTAINER || 'ctn_centos7',
  dockerLuaDir: process.env.KFS_DOCKER_LUA_DIR || '/root/project/KcpFrameSync/lua_test',
  localTokensDir: process.env.KFS_LOCAL_TOKENS_DIR || '',
};
let epCache = { at: 0, ep: null };
function detectServerEndpoint() {
  if (envServerHost) {
    const family = envServerFamily || (envServerHost.includes(':') ? 'udp6' : 'udp4');
    return { host: envServerHost, family, source: 'env' };
  }
  if (envServerFamily === 'udp6') return { host: '::1', family: 'udp6', source: 'env' };
  if (envServerFamily === 'udp4') return { host: '127.0.0.1', family: 'udp4', source: 'env' };
  const now = Date.now();
  if (epCache.ep && now - epCache.at < 2000) return epCache.ep;
  if (process.platform === 'win32') {
    try {
      const cmd = `(Get-NetUDPEndpoint -LocalPort ${cfg.serverPort} -ErrorAction SilentlyContinue | Select-Object -ExpandProperty LocalAddress) -join ','`;
      const out = execFileSync('powershell', ['-NoProfile', '-NonInteractive', '-Command', cmd], {
        encoding: 'utf8', timeout: 3000, windowsHide: true,
      }).trim();
      const addrs = out.split(',').map((s) => s.trim()).filter(Boolean);
      const has4 = addrs.some((a) => a === '0.0.0.0' || a.startsWith('127.'));
      const has6 = addrs.some((a) => a.includes(':'));
      let ep = null;
      if (has6) ep = { host: '::1', family: 'udp6', source: 'auto6' };
      else if (has4) ep = { host: '127.0.0.1', family: 'udp4', source: 'auto4' };
      if (ep) { epCache = { at: now, ep }; return ep; }
    } catch (e) { /* PowerShell 不可用，回退 netstat */ }
    try {
      const out = execFileSync('netstat', ['-ano', '-p', 'UDP'], {
        encoding: 'utf8', timeout: 2000, windowsHide: true,
      });
      const port = cfg.serverPort;
      const lines = out.split(/\r?\n/);
      const has4 = lines.some((l) =>
        new RegExp(`^\\s*UDP\\s+(0\\.0\\.0\\.0|127\\.0\\.0\\.1):${port}\\b`, 'i').test(l));
      const has6 = lines.some((l) =>
        new RegExp(`^\\s*UDP\\s+\\[::1?\\]:${port}\\b`, 'i').test(l));
      let ep = null;
      if (has6) ep = { host: '::1', family: 'udp6', source: 'auto6' };
      else if (has4) ep = { host: '127.0.0.1', family: 'udp4', source: 'auto4' };
      if (ep) { epCache = { at: now, ep }; return ep; }
    } catch (e) { /* 忽略，回退 IPv4 */ }
  }
  return { host: '127.0.0.1', family: 'udp4', source: 'fallback' };
}
const t0 = process.hrtime.bigint();
function nowMs() {
  return Number((process.hrtime.bigint() - t0) / 1000000n) >>> 0;
}
function fnv1a(buf) {
  let h = FNV_OFFSET_BASIS >>> 0;
  for (const b of buf) {
    h ^= b;
    h = Math.imul(h, FNV_PRIME) >>> 0;
  }
  return h;
}
function encodeFrame2(type, fid, data) {
  const out = Buffer.alloc(KFS_FRAME2_HEADER_SIZE + data.length);
  out[0] = type;
  out.writeUInt32LE(fid >>> 0, 1);
  out.writeUInt16LE(data.length, 5);
  data.copy(out, KFS_FRAME2_HEADER_SIZE);
  return out;
}
function decodeFrame2(pkt) {
  if (pkt.length < KFS_FRAME2_HEADER_SIZE) return null;
  const type = pkt[0];
  const fid = pkt.readUInt32LE(1);
  const dlen = pkt.readUInt16LE(5);
  if (pkt.length !== KFS_FRAME2_HEADER_SIZE + dlen) return null;
  return { type, fid, data: pkt.subarray(KFS_FRAME2_HEADER_SIZE) };
}
//---------------------------------------------------------------------
// 会话：一个 WebSocket 连接 = 一个虚拟 kfs 客户端
//---------------------------------------------------------------------
class Session {
  constructor(ws) {
    this.ws = ws;
    this.closed = false;
    this.socket = null;
    this.kcp = null;
    this.conv = 0;
    this.roomId = 0;
    this.token = 0;
    this.heartbeatMs = 0;
    this.joined = false;
    this.joinOk = false;
    this.udpRx = 0;
    this.lastHb = 0;
    this.updateTimer = null;
    this.joinTimer = null;
    this.id = Session.nextId++;
  }
  sendJson(obj) {
    if (this.closed || this.ws.readyState !== 1) return;
    try { this.ws.send(JSON.stringify(obj)); } catch (e) { /* 忽略 */ }
  }
  log(level, msg) {
    console.log(`[gw#${this.id}] ${msg}`);
    this.sendJson({ op: 'log', level, msg });
  }
  sendUdp(buf) {
    if (!this.socket || this.closed) return;
    this.socket.send(buf, 0, buf.length, cfg.serverPort, this.serverHost || cfg.serverHost, (err) => {
      if (err) this.log('error', `udp send: ${err.message}`);
    });
  }
  kcpSend(frame) {
    if (!this.kcp || this.closed) return;
    const r = ikcp.send(this.kcp, frame);
    if (r !== 0) this.log('error', `ikcp_send 失败 r=${r}`);
  }
  handleJoin(msg) {
    if (this.socket) {
      if (this.updateTimer) { clearInterval(this.updateTimer); this.updateTimer = null; }
      if (this.joinTimer) { clearTimeout(this.joinTimer); this.joinTimer = null; }
      if (this.kcp) { ikcp.release(this.kcp); this.kcp = null; }
      try { this.socket.close(); } catch (e) { /* 忽略 */ }
      this.socket = null;
      this.joined = false; this.joinOk = false;
      this.udpRx = 0;
      this.closed = false;
      this.log('info', `重试 join：已清理旧会话，将重新发起握手`);
    }
    this.roomId = Number(msg.room_id) >>> 0;
    this.conv = Number(msg.conv) >>> 0;
    this.token = Number(msg.token || 0) >>> 0;
    this.heartbeatMs = Math.max(0, Number(msg.heartbeat_ms || 0));
    const ep = detectServerEndpoint();
    this.serverHost = ep.host;
    this.udpFamily = ep.family;
    this.socket = dgram.createSocket(ep.family);
    this.socket.on('message', (buf) => this.onUdp(buf));
    this.socket.on('error', (e) => this.log('error', `udp: ${e.message}`));
    this.log('info', `UDP 目标 ${ep.host}:${cfg.serverPort} (${ep.family}, ${ep.source})`);
    this.socket.bind(0, () => {
      this.kcp = ikcp.create(this.conv, this);
      this.kcp.output = (buf, len) => {
        this.sendUdp(buf.subarray(0, len));
        return len;
      };
      ikcp.nodelay(this.kcp, 1, 10, 2, 1);
      const hs = Buffer.alloc(KFS_HS_LEN2);
      hs.writeUInt32LE(KFS_HANDSHAKE_MAGIC, 0);
      hs.writeUInt32LE(this.roomId, 4);
      hs.writeUInt32LE(this.conv, 8);
      hs.writeUInt32LE(this.token, 12);
      this.log('info',`握手包原始hex:${hs.toString('hex')}`);
      this.sendUdp(hs);
      this.log('info', `JOIN 已发送 room_id=${this.roomId} conv=${this.conv} token=${this.token}`);
      this.joinTimer = setTimeout(() => {
        if (this.joined) return;
        let hint;
        if (this.udpRx === 0) {
          const target = `${this.serverHost || cfg.serverHost}:${cfg.serverPort}`;
          hint =
            `2s 内没有收到任何 UDP 回包：${target}/udp (${this.udpFamily || cfg.serverFamily}) 不可达。\n` +
            `      先确认 server.lua 在跑（容器内: pgrep -af 'lua server.lua'），且它监听的 udp_port 与网关目标端口一致。\n` +
            (cfg.serverPort === 10500
              ? `      当前目标是 10501：这是 bridge_windows.js 的 UDP 口，必须同时有 bridge_container.js 在容器内运行；\n` +
                `      直连 Docker 服务器请把 KFS_SERVER_PORT 改回 10500（或看 lua_test/config 的 udp_port）。\n`
              : `      网关在容器内时目标应是 127.0.0.1:${cfg.serverPort}；网关在 Windows 时先查 Docker 的 UDP 代理：\n` +
                `      netstat -ano -p UDP | findstr :${cfg.serverPort}（没有输出就 docker restart ${cfg.dockerContainer}）。\n`) +
            `      端口可用 KFS_SERVER_PORT 覆盖；网关启动 banner 会打印实际目标端口。`;
        } else {
          hint =
            `收到 ${this.udpRx} 个 UDP 包但没有 JOIN 应答：请确认 room_id=${this.roomId} 已由 server.lua 创建、` +
            `容器内只有一个 server.lua 在跑，且 token 是本次启动的 tokens_${this.roomId}.txt 里的值。`;
        }
        this.log('warn', `JOIN 应答超时(${cfg.joinTimeoutMs}ms) room_id=${this.roomId} conv=${this.conv}\n      ${hint}`);
        this.sendJson({ op: 'joined', ok: false, result: -1, msg: hint });
      }, cfg.joinTimeoutMs);
      this.lastHb = nowMs();
      this.updateTimer = setInterval(() => this.tick(), cfg.updateMs);
    });
  }
  handleTokens(msg) {
    const room = Number(msg.room_id) >>> 0;
    if (!room) {
      this.sendJson({ op: 'tokens', room_id: room, error: 'invalid room_id' });
      return;
    }
    const dir = cfg.localTokensDir || cfg.dockerLuaDir;
    const tokenPath = `${dir}/tokens_${room}.txt`;
    const configPath = `${dir}/config`;
    console.log(`[gw#${this.id}] 读取 token/config: room=${room} (${dir}${cfg.localTokensDir ? ', local' : ', docker'})`);
    const send = (tokensText, configText) => {
      const tokens = [];
      for (const line of String(tokensText).split(/\r?\n/)) {
        const m = line.match(/^(\d+)\s+(\d+)/);
        if (m) tokens.push({ conv: Number(m[1]), token: Number(m[2]) });
      }
      const fm = String(configText || '').match(/^\s*frame_rate\s*=\s*(\d+)/m);
      const frameRate = fm ? Number(fm[1]) : 0;
      console.log(`[gw#${this.id}] 读取 token 成功: room=${room} 共 ${tokens.length} 个, frame_rate=${frameRate}`);
      this.sendJson({ op: 'tokens', room_id: room, tokens, frame_rate: frameRate });
    };
    const fail = (detail) => {
      const where = cfg.localTokensDir
        ? `容器内目录 ${dir}`
        : `容器 ${cfg.dockerContainer} 的 ${dir}`;
      const msg = `${detail}（${where}；容器名可用 KFS_DOCKER_CONTAINER 覆盖，目录可用 KFS_DOCKER_LUA_DIR 覆盖）`;
      console.log(`[gw#${this.id}] 读取 token 失败: ${msg}`);
      this.sendJson({ op: 'tokens', room_id: room, error: msg });
    };
    if (cfg.localTokensDir) {
      try {
        const tokensText = fs.readFileSync(tokenPath, 'utf8');
        const configText = fs.existsSync(configPath) ? fs.readFileSync(configPath, 'utf8') : '';
        send(tokensText, configText);
      } catch (e) {
        fail(e.message);
      }
      return;
    }
    execFile('docker', ['exec', cfg.dockerContainer, 'sh', '-c',
      `cat '${tokenPath}'; echo '__CFG__'; cat '${configPath}' 2>/dev/null || true`],
      { timeout: 3000 }, (err, stdout, stderr) => {
        if (err) {
          fail((stderr || err.message || 'docker exec failed').trim());
          return;
        }
        const parts = String(stdout).split('**CFG**');
        send(parts[0] || '', parts[1] || '');
      });
  }
  // UDP 收到数据：先判裸握手应答，其余喂 KCP
  onUdp(buf) {
    this.udpRx++;
    // 字节序：服务器 memcpy 写 ACK（x86 小端），线上 magic 为小端，必须 readUInt32LE。
    if (buf.length >= 5 && buf.readUInt32LE(0) === KFS_HANDSHAKE_MAGIC) {
      const result = buf[4];
      this.joined = true;
      this.joinOk = (result === 0);
      if (this.joinTimer) { clearTimeout(this.joinTimer); this.joinTimer = null; }
      const resultNames = ['成功', '房间不存在', '房间已满', 'conv冲突', '其他错误', 'token错误'];
      const out = { op: 'joined', ok: this.joinOk, result };
      // 扩展应答（21B）：[magic4][result1][frame_interval_ms4][room_seed4][server_time_ms8]
      //   重连场景浏览器可用它对齐帧间隔、用 room_seed 回滚本地 RNG。
      if (buf.length >= 21) {
        out.frame_interval_ms = buf.readUInt32LE(5) >>> 0;
        out.room_seed = buf.readUInt32LE(9) >>> 0;
        out.server_time_ms = Number(buf.readBigUInt64LE(13));
      }
      this.log('info', `JOIN 应答 result=${result}(${resultNames[result] || '未知'}) conv=${this.conv}` +
        (out.frame_interval_ms ? ` frame_interval_ms=${out.frame_interval_ms} seed=${out.room_seed}` : ''));
      this.sendJson(out);
      return;
    }
    if (this.kcp) ikcp.input(this.kcp, buf);
  }
  // 驱动 tick：update KCP → 自动心跳 → 收帧并按 type 分发转发
  tick() {
    const now = nowMs();
    if (this.kcp) ikcp.update(this.kcp, now);
    if (this.kcp && this.heartbeatMs > 0) {
      const elapsed = ((now - this.lastHb) >>> 0);
      if (elapsed > this.heartbeatMs) {
        this.kcpSend(encodeFrame2(KFS_CMD.HEARTBEAT, 0, Buffer.alloc(0)));
        this.lastHb = now;
      }
    }
    // 收帧分发：BROADCAST 对账转发；REJOIN_SYNC/PLAYER_REJOIN 转发浏览器；其余控制帧忽略。
    if (this.kcp) {
      let raw;
      while ((raw = ikcp.recv(this.kcp)) !== null) {
        const f = decodeFrame2(raw);
        if (!f) continue;
        if (f.type === KFS_CMD.BROADCAST) {
          let ok = false;
          if (f.data.length >= 4) {
            const hSent = f.data.readUInt32LE(f.data.length - 4);
            const hCalc = fnv1a(f.data.subarray(0, f.data.length - 4));
            ok = (hCalc === hSent);
          }
          this.log('info', `收到服务器广播 fid=${f.fid} 对账=${ok ? '一致' : '不一致!!'}`);
          this.sendJson({ op: 'broadcast', fid: f.fid >>> 0, payload: f.data.toString('hex'), ok });
        } else if (f.type === KFS_CMD.REJOIN_SYNC) {
          // 锚点同步包负载 = [sync_fid 4][collect_fid 4][room_seed 4][frame_interval_ms 4]（与 C 端一致）
          const d = f.data;
          if (d.length >= 16) {
            const obj = {
              op: 'rejoin_sync',
              fid: f.fid >>> 0,
              sync_fid: d.readUInt32LE(0) >>> 0,
              collect_fid: d.readUInt32LE(4) >>> 0,
              room_seed: d.readUInt32LE(8) >>> 0,
              frame_interval_ms: d.readUInt32LE(12) >>> 0,
            };
            this.log('info', `重连同步包 sync_fid=${obj.sync_fid} collect_fid=${obj.collect_fid} ` +
              `seed=${obj.room_seed} interval=${obj.frame_interval_ms}`);
            this.sendJson(obj);
          } else {
            this.log('warn', `REJOIN_SYNC 负载长度异常 len=${d.length}（期望 >=16），已忽略`);
          }
        } else if (f.type === KFS_CMD.PLAYER_REJOIN) {
          // 负载 = [conv 4B]，通知其他玩家该 conv 已重连上线
          if (f.data.length >= 4) {
            const conv = f.data.readUInt32LE(0) >>> 0;
            this.log('info', `玩家重连上线通知 conv=${conv} fid=${f.fid}`);
            this.sendJson({ op: 'player_rejoin', fid: f.fid >>> 0, conv });
          }
        }
        // 其它控制帧（HEARTBEAT/READY/STATE 等服务器一般不主动下发）忽略
      }
    }
  }
  handleMessage(text) {
    let msg;
    try { msg = JSON.parse(text); } catch (e) {
      this.sendJson({ op: 'error', msg: 'bad json' });
      return;
    }
    switch (msg.op) {
      case 'tokens':
        this.handleTokens(msg);
        break;
      case 'join':
        this.handleJoin(msg);
        break;
      case 'input': {
        const data = Buffer.from(String(msg.data || ''), 'hex');
        if (!this.kcp) break;
        this.kcpSend(encodeFrame2(KFS_CMD.INPUT, Number(msg.fid) >>> 0, data));
        break;
      }
      case 'ready':
        if (this.kcp) this.kcpSend(encodeFrame2(KFS_CMD.READY, 0, Buffer.alloc(0)));
        break;
      case 'state': {
        const hb = Buffer.alloc(4);
        hb.writeUInt32LE(Number(msg.hash) >>> 0, 0);
        if (this.kcp) this.kcpSend(encodeFrame2(KFS_CMD.STATE, Number(msg.fid) >>> 0, hb));
        break;
      }
      case 'heartbeat':
        if (this.kcp) {
          this.kcpSend(encodeFrame2(KFS_CMD.HEARTBEAT, 0, Buffer.alloc(0)));
          this.lastHb = nowMs();
        }
        break;
      case 'leave':
        if (this.kcp) this.kcpSend(encodeFrame2(KFS_CMD.LEAVE, 0, Buffer.alloc(0)));
        break;
      case 'ping':
        this.sendJson({ op: 'pong' });
        break;
      default:
        this.sendJson({ op: 'error', msg: 'unknown op: ' + msg.op });
    }
  }
  cleanup() {
    if (this.closed) return;
    this.closed = true;
    if (this.updateTimer) clearInterval(this.updateTimer);
    if (this.joinTimer) clearTimeout(this.joinTimer);
    if (this.kcp) { ikcp.release(this.kcp); this.kcp = null; }
    if (this.socket) { try { this.socket.close(); } catch (e) { /* 忽略 */ } this.socket = null; }
    if (this.conv === 0 && !this.joined) {
      console.log(`[gw#${this.id}] 临时连接关闭（未 JOIN，通常是"读取当前 token"）`);
    } else {
      this.log('info', `会话关闭 conv=${this.conv}`);
    }
  }
}
Session.nextId = 1;
//---------------------------------------------------------------------
// WebSocket 服务器
//---------------------------------------------------------------------
const wss = new WebSocketServer({ port: cfg.wsPort });
wss.on('connection', (ws) => {
  const s = new Session(ws);
  console.log(`[gw] 浏览器连接 ${ws._socket ? ws._socket.remoteAddress + ':' + ws._socket.remotePort : ''} → 会话#${s.id}`);
  ws.on('message', (data) => s.handleMessage(data.toString('utf8')));
  ws.on('close', () => s.cleanup());
  ws.on('error', () => s.cleanup());
});
wss.on('listening', () => {
  const ep = detectServerEndpoint();
  console.log('==============================================================');
  console.log(` kfs 网关已启动`);
  console.log(`   WebSocket  : ws://127.0.0.1:${cfg.wsPort}   (浏览器连接)`);
  console.log(`   UDP 目标   : ${ep.host}:${cfg.serverPort} (${ep.family}, ${ep.source})  ← Docker kfs 服务器`);
  console.log(`   驱动节奏   : ${cfg.updateMs}ms  join超时 ${cfg.joinTimeoutMs}ms`);
  console.log(`   重连转发   : REJOIN_SYNC(9)→rejoin_sync  PLAYER_REJOIN(10)→player_rejoin`);
  if (ep.source === 'fallback') {
    console.log(`   提示       : 启动时没检测到 UDP ${cfg.serverPort} 监听，JOIN 时会重新探测（可先启动 server.lua）`);
  }
  console.log('==============================================================');
});
wss.on('error', (e) => {
  console.error(`[gw] WebSocket 服务器错误: ${e.message}`);
  process.exit(1);
});