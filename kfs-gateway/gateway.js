//=====================================================================
// gateway.js — 浏览器 WebSocket → UDP/KCP → Docker kfs 帧同步服务器 网关
//
// 架构：
//   浏览器(WebSocket)  ⇄  gateway.js(本机 Node)  ⇄  Docker 内的 kfs 服务器(UDP)
//
// 每个 WebSocket 连接 = 一个"虚拟 kfs 客户端"：
//   - 独立 UDP socket(bind 0，系统分配端口，与 C 客户端 net_open(0) 一致)
//   - 独立 KCP 会话(ikcp.js，与 ikcp.c 字节级兼容)
//   - 复刻 C 客户端驱动线程行为：UDP 收包喂 KCP → ikcp_update →
//     自动补发 HEARTBEAT → recv 广播帧并做 FNV-1a 对账校验
//
// 裸 UDP JOIN 握手：
//   请求 [magic 4B "KFCF"][room_id 4B][conv 4B][token 4B]  (16B)
//   应答 [magic 4B][result 1B]  result: 0成功 1不存在 2满 3冲突 4其他 5token错
//
// KCP 会话内帧（frame2 信封）：
//   [type 1B][fid 4B][len 2B][data...]
//   type: 1=INPUT 2=BROADCAST 3=JOIN 4=LEAVE 5=HEARTBEAT 6=STATE 7=READY
//   广播负载 = 逐玩家 [conv 4B][len 2B][data] + 末尾 4B FNV-1a32(body)
//
// WebSocket 消息协议（文本 JSON，二进制字段用 hex）：
//   浏览器→网关: {op:"join",room_id,conv,token,heartbeat_ms}
//               {op:"tokens",room_id}  ← 读取容器内当前 tokens_<room>.txt
//               {op:"input",fid,data}  {op:"ready"}  {op:"state",fid,hash}
//               {op:"heartbeat"}  {op:"leave"}  {op:"ping"}
//   网关→浏览器: {op:"joined",ok,result}  {op:"broadcast",fid,payload,ok}
//               {op:"tokens",room_id,tokens|error}
//               {op:"log",level,msg}  {op:"error",msg}  {op:"pong"}
//
// 环境变量配置：
//   KFS_SERVER_HOST   默认 127.0.0.1（Docker 把 10500/udp 映射到宿主机时用本机地址）
//   KFS_SERVER_PORT   默认 10500
//   WS_PORT           默认 8081
//   KFS_UPDATE_MS     默认 5（驱动节奏，对齐 C 客户端 1ms 节流）
//   KFS_JOIN_TIMEOUT_MS 默认 2000（对齐 C 客户端 join 等待超时）
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
};
const FNV_OFFSET_BASIS = 2166136261;
const FNV_PRIME = 16777619;

const envServerHost = process.env.KFS_SERVER_HOST || '';
const envServerFamily = process.env.KFS_SERVER_FAMILY || '';

// 网关目标 UDP 端口：KFS_SERVER_PORT > <项目>/lua_test/config 的 udp_port > 10500。
// 目录调整后如果只改了 config 的端口，网关也能自动跟上，不会再出现
// “服务器在跑却 JOIN 超时”（端口和目标对不上）。
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
  joinTimeoutMs: parseInt(process.env.KFS_JOIN_TIMEOUT_MS || '2000', 10),
  dockerContainer: process.env.KFS_DOCKER_CONTAINER || 'ctn_centos7',
  dockerLuaDir: process.env.KFS_DOCKER_LUA_DIR || '/root/project/KcpFrameSync/lua_test',
  // 网关跑在容器内时，直接读本地 tokens_<room>.txt（不需要 docker exec）
  localTokensDir: process.env.KFS_LOCAL_TOKENS_DIR || '',
};

// 选择 UDP 目标地址族。
//   Docker Desktop 有时把 -p 10500:10500/udp 发布成 IPv6-only 的 [::]:10500，
//   此时网关用 127.0.0.1 + udp4 发 JOIN 会完全收不到回包（表现为 JOIN 超时）。
//   Get-NetUDPEndpoint 比 netstat 更可靠（Docker 的双栈 socket 会显示成 ::，
//   而 netstat 可能显示 0.0.0.0）；按它自动选 IPv4/IPv6，可用
//   KFS_SERVER_HOST / KFS_SERVER_FAMILY 手动覆盖。
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
    // 首选 PowerShell：Get-NetUDPEndpoint 能反映 Docker 代理真实的地址族
    try {
      const cmd = `(Get-NetUDPEndpoint -LocalPort ${cfg.serverPort} -ErrorAction SilentlyContinue | Select-Object -ExpandProperty LocalAddress) -join ','`;
      const out = execFileSync('powershell', ['-NoProfile', '-NonInteractive', '-Command', cmd], {
        encoding: 'utf8', timeout: 3000, windowsHide: true,
      }).trim();
      const addrs = out.split(',').map((s) => s.trim()).filter(Boolean);
      const has4 = addrs.some((a) => a === '0.0.0.0' || a.startsWith('127.'));
      const has6 = addrs.some((a) => a.includes(':'));
      let ep = null;
      // 双栈时优先 IPv6：Docker Desktop 的 :: 是当前最稳定的发布地址
      if (has6) ep = { host: '::1', family: 'udp6', source: 'auto6' };
      else if (has4) ep = { host: '127.0.0.1', family: 'udp4', source: 'auto4' };
      if (ep) {
        epCache = { at: now, ep };
        return ep;
      }
    } catch (e) { /* PowerShell 不可用，回退 netstat */ }

    // 回退：netstat
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
      if (ep) {
        epCache = { at: now, ep };
        return ep;
      }
    } catch (e) { /* 忽略，回退 IPv4 */ }
  }
  return { host: '127.0.0.1', family: 'udp4', source: 'fallback' };
}

// 相对单调时钟（uint32 毫秒，对齐 kfs_now_ms / ikcp current）
const t0 = process.hrtime.bigint();
function nowMs() {
  return Number((process.hrtime.bigint() - t0) / 1000000n) >>> 0;
}

// FNV-1a 32（frame_fnv1a）
function fnv1a(buf) {
  let h = FNV_OFFSET_BASIS >>> 0;
  for (const b of buf) {
    h ^= b;
    h = Math.imul(h, FNV_PRIME) >>> 0;
  }
  return h;
}

// frame_encode2： [type 1B][fid 4B LE][len 2B LE][data]
function encodeFrame2(type, fid, data) {
  const out = Buffer.alloc(KFS_FRAME2_HEADER_SIZE + data.length);
  out[0] = type;
  out.writeUInt32LE(fid >>> 0, 1);
  out.writeUInt16LE(data.length, 5);
  data.copy(out, KFS_FRAME2_HEADER_SIZE);
  return out;
}

// frame_decode2
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
    this.joined = false;      // 已收到服务器 JOIN 应答
    this.joinOk = false;      // JOIN 是否成功
    this.udpRx = 0;           // 本会话收到的 UDP 包数（排查“完全没回包”用）
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

  // 发往 kfs 服务器（对齐 net_send → setpeer 的服务器地址）
  sendUdp(buf) {
    // this.log('info', `JOIN 发送 room_id=${this.roomId} conv=${this.conv} token=${this.token}`);
    // this.log('info', `111 sendUdp: ${cfg.serverHost}:${cfg.serverPort} buf:${buf}`);
    if (!this.socket || this.closed) return;
    // this.log('info', `222 sendUdp: ${cfg.serverHost}:${cfg.serverPort} buf:${buf}`);
    this.socket.send(buf, 0, buf.length, cfg.serverPort, this.serverHost || cfg.serverHost, (err) => {
      if (err) this.log('error', `udp send: ${err.message}`);
    });
  }

  // 经 KCP 发送一帧（对齐 ikcp_send）
  kcpSend(frame) {
    if (!this.kcp || this.closed) return;
    const r = ikcp.send(this.kcp, frame);
    if (r !== 0) this.log('error', `ikcp_send 失败 r=${r}`);
  }

  // join(room_id, conv, token)：创建 UDP+KCP，发裸 JOIN 握手
  handleJoin(msg) {
    // 支持重试 join：若已有旧会话（上次 JOIN 超时/失败后 WS 仍开着），先清理再重建
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
      // 创建 KCP 会话（对齐 kfs.new(0, conv) + nodelay(1,10,2,1)）
      this.kcp = ikcp.create(this.conv, this);
      this.kcp.output = (buf, len) => {
        this.sendUdp(buf.subarray(0, len));
        return len;
      };
      ikcp.nodelay(this.kcp, 1, 10, 2, 1);

      // 裸 UDP JOIN 握手
      const hs = Buffer.alloc(KFS_HS_LEN2);
      hs.writeUInt32LE(KFS_HANDSHAKE_MAGIC, 0); // magic 改回小端
      hs.writeUInt32LE(this.roomId, 4);
      hs.writeUInt32LE(this.conv, 8);
      hs.writeUInt32LE(this.token, 12);
      this.log('info',`握手包原始hex:${hs.toString('hex')}`);
      this.sendUdp(hs);
      this.log('info', `JOIN 已发送 room_id=${this.roomId} conv=${this.conv} token=${this.token}`);

      // 等待服务器应答（对齐 C 客户端 join 2s 超时）
      this.joinTimer = setTimeout(() => {
        if (this.joined) return;
        // 区分“一个 UDP 包都没收到”和“收到了包但没有 JOIN 应答”：
        //   前者说明网关目标不可达：容器内网关一般是 server.lua 没起/端口不一致，
        //   Windows 网关则多半是 Docker 的 UDP 端口代理丢失；后者才是 room/token 问题。
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

      // 驱动定时器（对齐 C 驱动线程：收包喂 KCP → update → 心跳 → 收广播）
      this.lastHb = nowMs();
      this.updateTimer = setInterval(() => this.tick(), cfg.updateMs);
    });
  }

  // 读取容器内本次 server.lua 启动生成的 tokens_<room>.txt 和 config 的 frame_rate
  //   仅用于网页登录框“读取当前 token / 自动对齐帧间隔”，避免旧 token 和
  //   页面 step_ms 与服务器 frame_rate 不一致导致移动距离看起来不同。
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

    // 网关跑在容器内：直接读本地文件，不依赖 docker CLI
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

    // 网关跑在 Windows：docker exec 读容器内文件
    execFile('docker', ['exec', cfg.dockerContainer, 'sh', '-c',
      `cat '${tokenPath}'; echo '__CFG__'; cat '${configPath}' 2>/dev/null || true`],
      { timeout: 3000 }, (err, stdout, stderr) => {
        if (err) {
          fail((stderr || err.message || 'docker exec failed').trim());
          return;
        }
        const parts = String(stdout).split('__CFG__');
        send(parts[0] || '', parts[1] || '');
      });
  }

  // UDP 收到数据：先判裸握手应答，其余喂 KCP
  onUdp(buf) {
    this.udpRx++;
    // 字节序：服务器端 kfs_core.c 用 memcpy 写 ACK（x86 小端），故线上应答 magic 是小端，
    // 必须 readUInt32LE。之前误用 readUInt32BE 导致永远检测不到应答、握手超时。
    if (buf.length >= 5 && buf.readUInt32LE(0) === KFS_HANDSHAKE_MAGIC) {
        const result = buf[4];
        this.joined = true;
        this.joinOk = (result === 0);
        if (this.joinTimer) { clearTimeout(this.joinTimer); this.joinTimer = null; }
        const resultNames = ['成功', '房间不存在', '房间已满', 'conv冲突', '其他错误', 'token错误'];
        this.log('info', `JOIN 应答 result=${result}(${resultNames[result] || '未知'}) conv=${this.conv}`);
        this.sendJson({ op: 'joined', ok: this.joinOk, result });
        return;
    }
    if (this.kcp) ikcp.input(this.kcp, buf);
  }

  // 驱动 tick：update KCP → 自动心跳 → 收广播转发
  tick() {
    const now = nowMs();
    if (this.kcp) ikcp.update(this.kcp, now);

    // 自动心跳（对齐 C 驱动线程：超过 heartbeat_ms 未发则补发空 HEARTBEAT 帧）
    if (this.kcp && this.heartbeatMs > 0) {
      const elapsed = ((now - this.lastHb) >>> 0);
      if (elapsed > this.heartbeatMs) {
        this.kcpSend(encodeFrame2(KFS_CMD.HEARTBEAT, 0, Buffer.alloc(0)));
        this.lastHb = now;
      }
    }

    // 收广播（对齐 recv_broadcast：只处理 BROADCAST 帧，其余丢弃）
    if (this.kcp) {
      let raw;
      while ((raw = ikcp.recv(this.kcp)) !== null) {
        const f = decodeFrame2(raw);
        if (!f) continue;
        if (f.type !== KFS_CMD.BROADCAST) continue;
        let ok = false;
        if (f.data.length >= 4) {
          const hSent = f.data.readUInt32LE(f.data.length - 4);
          const hCalc = fnv1a(f.data.subarray(0, f.data.length - 4));
          ok = (hCalc === hSent);
        }
        this.log('info', `收到服务器广播 fid=${f.fid} 对账=${ok ? '一致' : '不一致!!'}`);
        this.sendJson({
          op: 'broadcast',
          fid: f.fid >>> 0,
          payload: f.data.toString('hex'),
          ok,
        });
      }
    }
  }

  // 浏览器 → 网关消息
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
      console.log(`[gw#${this.id}] 临时连接关闭（未 JOIN，通常是“读取当前 token”）`);
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
  if (ep.source === 'fallback') {
    console.log(`   提示       : 启动时没检测到 UDP ${cfg.serverPort} 监听，JOIN 时会重新探测（可先启动 server.lua）`);
  }
  console.log('==============================================================');
});

wss.on('error', (e) => {
  console.error(`[gw] WebSocket 服务器错误: ${e.message}`);
  process.exit(1);
});
