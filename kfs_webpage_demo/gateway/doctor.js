//=====================================================================
// doctor.js — kfs 网页 Demo 联调诊断（Windows 宿主机）
//
// 直接回答“为什么 JOIN 超时”：
//   1. Windows 宿主机上到底有没有 UDP 10500 监听（Docker 端口代理是否生效）
//   2. Docker 容器是否真的发布了 10500/udp
//   3. 容器里是不是跑了多个 server.lua（SO_REUSEADDR 会静默分流）
//   4. 读取本次 server.lua 启动生成的 tokens_<room>.txt
//   5. 用这些 token 从宿主机发 JOIN 探测，确认整条 UDP 链路
//
// 用法（在 gateway 目录）：
//   node doctor.js                # 默认 127.0.0.1:10500 房间 1
//   node doctor.js 2              # 房间 2
//   set KFS_DOCKER_CONTAINER=xxx  # 容器名不叫 ctn_centos7 时指定
//   set KFS_DOCKER_LUA_DIR=...    # 容器内 lua_test 目录
//   set KFS_SERVER_HOST=...       # 服务器不在本机时
//=====================================================================
'use strict';

const dgram = require('dgram');
const { execFileSync } = require('child_process');

// 有 ws 模块时用真正的 WebSocket 探测：Docker 的 TCP 端口代理会“接受连接”，
// 即使容器里根本没有网关在监听 18080，net.connect 也会显示“可连接”，非常误导。
let WebSocket = null;
try { WebSocket = require('ws'); } catch (e) { /* 没有 ws 就退回 TCP 探测 */ }

const LUA_DIR = process.env.KFS_DOCKER_LUA_DIR || '/root/project/KcpFrameSync/lua_test';

const cfg = {
  host: process.env.KFS_SERVER_HOST || '127.0.0.1',
  port: parseInt(process.env.KFS_SERVER_PORT || '10500', 10),
  container: process.env.KFS_DOCKER_CONTAINER || 'ctn_centos7',
  luaDir: LUA_DIR,
  gatewayDir: process.env.KFS_CONTAINER_GATEWAY_DIR
    || LUA_DIR.replace(/\/lua_test\/?$/, '/kfs-gateway'),
  room: parseInt(process.env.KFS_ROOM_ID || process.argv[2] || '1', 10),
};

const MAGIC = 0x4B464346;
const HS_LEN2 = 16;
const RESULT_NAMES = ['成功', '房间不存在', '房间已满', 'conv冲突', '其他错误', 'token错误'];

function run(cmd, args) {
  try {
    const out = execFileSync(cmd, args, {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
    });
    return { ok: true, out: String(out).trim(), err: '' };
  } catch (e) {
    return {
      ok: false,
      out: e.stdout ? String(e.stdout).trim() : '',
      err: e.stderr ? String(e.stderr).trim() : (e.message || String(e)),
    };
  }
}

function hr(title) {
  console.log(`\n=== ${title} ===`);
}

// 用 {op:"tokens"} 真正问一次网关：能回 tokens 才说明容器里 gateway.js 活着，
// 而不只是 Docker 在 18080 上开了个 TCP 代理。
function probeGateway(port, room) {
  return new Promise((resolve) => {
    if (!WebSocket) {
      return resolve({ ok: false, reason: '未安装 ws 模块（在 gateway 目录执行 npm install）' });
    }
    const ws = new WebSocket(`ws://127.0.0.1:${port}`);
    let done = false;
    const finish = (v) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      try { ws.terminate(); } catch (e) { /* 忽略 */ }
      resolve(v);
    };
    const timer = setTimeout(() => finish({
      ok: false, reason: '3s 内没有收到 WebSocket 应答（TCP 能连，但容器里没有网关在监听）',
    }), 3000);
    ws.on('open', () => ws.send(JSON.stringify({ op: 'tokens', room_id: room })));
    ws.on('message', (data) => {
      let m;
      try { m = JSON.parse(String(data)); } catch (e) { return; }
      if (m.op === 'tokens') finish({ ok: true, tokens: m.tokens || [], error: m.error || '' });
    });
    ws.on('error', (e) => finish({ ok: false, reason: e.message }));
  });
}

function checkWindowsUdp() {
  hr(`Windows 宿主机 UDP ${cfg.port} 监听`);
  const r = run('netstat', ['-ano', '-p', 'UDP']);
  if (!r.ok && !r.out) {
    console.log(`  ✗ 无法执行 netstat: ${r.err}`);
    return { ok: false, pid: null, raw: '' };
  }
  const line = r.out.split(/\r?\n/).find((l) => /^\s*UDP\s+/i.test(l) && l.includes(`:${cfg.port}`));
  if (!line) {
    console.log(`  ✗ 没有找到 UDP ${cfg.port} 监听 —— 这就是 JOIN 超时的根因：`);
    console.log(`    JOIN 包根本到不了 Docker，服务器再正常也没用。`);
    console.log(`    修复: docker restart ${cfg.container}  （或重启 Docker Desktop）`);
    console.log(`    然后重新启动容器里的 server.lua。`);
    return { ok: false, pid: null, raw: '' };
  }
  const pid = (line.match(/(\d+)\s*$/) || [])[1] || '';
  console.log(`  ✓ ${line.trim()}`);
  return { ok: true, pid, raw: line };
}

function findContainer() {
  hr('Docker 容器 / 端口映射');
  const ps = run('docker', ['ps', '--format', '{{.Names}}\t{{.Ports}}']);
  if (!ps.ok) {
    console.log(`  ! 无法执行 docker ps: ${ps.err}`);
    console.log(`    如果 Docker CLI 不在 PATH，可只检查 Windows 侧；或手动确认容器。`);
    return { name: cfg.container, mapping: '', ok: false };
  }
  const lines = ps.out.split(/\r?\n/).filter(Boolean);
  let name = cfg.container;
  let mapping = '';
  const line = lines.find((l) => l.split('\t')[0] === cfg.container);
  if (line) {
    mapping = line.split('\t')[1] || '';
  } else {
    const candidate = lines.find((l) => l.includes(`->${cfg.port}/udp`));
    if (candidate) {
      const parts = candidate.split('\t');
      name = parts[0];
      mapping = parts[1] || '';
      console.log(`  ! 配置的容器 ${cfg.container} 未运行，自动改用 ${name}`);
    }
  }
  if (!line && !mapping) {
    console.log(`  ✗ 没有找到运行中的容器 ${cfg.container}，也没有容器发布 ${cfg.port}/udp`);
    return { name, mapping, ok: false };
  }
  const hasUdp = mapping.includes(`${cfg.port}/udp`);
  console.log(`  容器: ${name}`);
  console.log(`  端口: ${mapping || '(空)'}`);
  console.log(hasUdp ? `  ✓ 已发布 ${cfg.port}/udp` : `  ✗ 没有发布 ${cfg.port}/udp（启动容器时要加 -p ${cfg.port}:${cfg.port}/udp）`);
  return { name, mapping, ok: hasUdp };
}

function checkServers(container) {
  hr('容器内 server.lua 进程');
  const r = run('docker', ['exec', container, 'sh', '-c', "ps -eo pid,args | grep '[l]ua server.lua'"]);
  const lines = (r.out || '').split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
  if (lines.length === 0) {
    console.log('  ✗ 容器内没有 server.lua 在跑');
    console.log(`    启动: docker exec -d ${container} bash -lc "cd ${cfg.luaDir} && /root/project/KcpFrameSync/kfs_src/third/lua-5.4.8/src/lua server.lua 1 2 > server_run.log 2>&1"`);
    return 0;
  }
  for (const l of lines) console.log(`  ${l}`);
  if (lines.length > 1) {
    console.log(`  ✗ 发现 ${lines.length} 个 server.lua！UDP + SO_REUSEADDR 会让内核只把包投给其中一个，`);
    console.log(`    而且每个进程生成的 token 不同 —— 会出现“服务器在跑却超时/token 错”的假象。`);
    console.log(`    清理: docker exec ${container} sh -lc "pkill -f '[l]ua server.lua'"`);
    console.log(`    然后只启动一个。`);
  } else {
    console.log('  ✓ 只有一个 server.lua');
  }
  return lines.length;
}

function readTokens(container) {
  hr(`当前 token（room ${cfg.room}）`);
  const path = `${cfg.luaDir}/tokens_${cfg.room}.txt`;
  const r = run('docker', ['exec', container, 'cat', path]);
  if (!r.ok) {
    console.log(`  ✗ 读不到 ${path}: ${r.err}`);
    console.log(`    确认 server.lua 已创建房间 ${cfg.room}，且工作目录是 ${cfg.luaDir}`);
    return [];
  }
  const tokens = [];
  for (const line of (r.out || '').split(/\r?\n/)) {
    const m = line.match(/^(\d+)\s+(\d+)/);
    if (m) tokens.push({ conv: Number(m[1]), token: Number(m[2]) });
  }
  if (tokens.length === 0) {
    console.log(`  ✗ ${path} 里没有解析到 token`);
    return [];
  }
  for (const t of tokens) console.log(`  conv=${t.conv}  token=${t.token}`);
  return tokens;
}

function probe(conv, token, host, family) {
  return new Promise((resolve) => {
    const sock = dgram.createSocket(family || 'udp4');
    const hs = Buffer.alloc(HS_LEN2);
    hs.writeUInt32LE(MAGIC, 0);
    hs.writeUInt32LE(cfg.room >>> 0, 4);
    hs.writeUInt32LE(conv >>> 0, 8);
    hs.writeUInt32LE(token >>> 0, 12);
    let done = false;
    const finish = (v) => {
      if (done) return;
      done = true;
      try { sock.close(); } catch (e) { /* 忽略 */ }
      resolve(v);
    };
    sock.on('message', (buf) => {
      if (buf.length >= 5 && buf.readUInt32LE(0) === MAGIC) {
        finish({ result: buf[4], hex: buf.toString('hex') });
      }
    });
    sock.on('error', (e) => finish({ error: e.message }));
    sock.bind(0, host, () => {
      sock.send(hs, 0, hs.length, cfg.port, host, (err) => {
        if (err) finish({ error: err.message });
      });
    });
    setTimeout(() => finish(null), 1200);
  });
}

async function main() {
  console.log('==============================================================');
  console.log(` kfs Demo 诊断  ${cfg.host}:${cfg.port}/udp  room=${cfg.room}`);
  console.log('==============================================================');

  const win = checkWindowsUdp();
  const docker = findContainer();
  const container = docker.name || cfg.container;
  const count = docker.ok ? checkServers(container) : 0;
  const tokens = docker.ok ? readTokens(container) : [];

  hr('容器内网关（推荐路径 18080）');
  const gwProbe = await probeGateway(18080, cfg.room);
  const gw18080 = gwProbe.ok;
  let gwRunning = false;
  if (docker.ok) {
    const r = run('docker', ['exec', container, 'sh', '-c', "ps -eo pid,args | grep '[g]ateway.js'"]);
    gwRunning = !!(r.out && r.out.trim());
    if (r.out && r.out.trim()) console.log(`  ${r.out.trim()}`);
  }
  if (gw18080) {
    console.log(`  ✓ 18080 WebSocket 应答正常（token ${gwProbe.error ? '读取失败: ' + gwProbe.error : '读取成功，共 ' + gwProbe.tokens.length + ' 个'}）`);
  } else {
    console.log(`  ✗ 18080 不可用: ${gwProbe.reason}`);
  }
  console.log(gwRunning ? '  ✓ 容器内 gateway.js 在运行' : '  ✗ 容器内 gateway.js 没在运行');
  if (!gw18080 || !gwRunning) {
    console.log(`    修复: docker exec ${container} ${cfg.gatewayDir}/start_container.sh`);
    console.log(`    或:   docker exec ${container} env RESTART_GATEWAY=1 ${cfg.gatewayDir}/start_container.sh`);
  }

  hr('从宿主机发 JOIN 探测（本机 8081 网关路径）');
  // 用 conv=0 / token=0 探测：服务器会回 result=1/2/5（房间不存在 / conv 不在房间 /
  // token 校验拒绝），只要收到任一 ACK 就证明 UDP 链路通；conv=0 不会登记成真实玩家，
  // 因此不会让紧接着的浏览器 JOIN 收到 result=3（conv 已在线）。
  // Docker Desktop 可能只把 10500/udp 发布在 IPv6 [::] 上，所以 IPv4 不通时自动试 IPv6。
  const attempts = [];
  if (process.env.KFS_SERVER_HOST) {
    const h = process.env.KFS_SERVER_HOST;
    attempts.push({ host: h, family: h.includes(':') ? 'udp6' : 'udp4', label: 'env' });
  } else {
    attempts.push({ host: '127.0.0.1', family: 'udp4', label: 'IPv4' });
    attempts.push({ host: '::1', family: 'udp6', label: 'IPv6' });
  }
  let link = null, used = null;
  for (const a of attempts) {
    const r = await probe(0, 0, a.host, a.family);
    if (r && !r.error) {
      link = r; used = a;
      console.log(`  ✓ [${a.label}] ${a.host}:${cfg.port} 收到应答 result=${r.result} (${RESULT_NAMES[r.result] || '未知'})  hex=${r.hex}`);
      console.log('    ↑ 探测用的是 conv=0/token=0，被服务器拒绝是预期的；收到 ACK 只证明 UDP 链路通。');
      break;
    }
    console.log(`  - [${a.label}] ${a.host}:${cfg.port} 无应答${r && r.error ? ` (${r.error})` : ''}，尝试下一个地址族...`);
  }
  if (!link) {
    console.log('  ✗ IPv4 / IPv6 都无应答：UDP 链路不通，或服务器还没有任何房间');
  } else {
    cfg.host = used.host;
    cfg.family = used.family;
  }
  if (tokens.length > 0) {
    console.log(`  已读取 ${tokens.length} 个 token；探测包用的是 conv=0，不会占用玩家席位。`);
  }

  hr('结论');
  if (gw18080 && gwRunning) {
    console.log('  推荐路径 18080 正常：网页网关地址填 ws://127.0.0.1:18080，Ctrl+F5 后点②。');
    console.log('  下面的 10500 UDP 探测只用于本机 8081 网关，失败可以忽略。');
  } else {
    console.log('  容器内网关没起来：这就是网页“读取 token 失败：网关未启动？”的直接原因。');
    console.log(`  先执行: docker exec ${container} ${cfg.gatewayDir}/start_container.sh`);
    if (!win.ok) {
      console.log('  另外 Windows 宿主机没有 UDP 10500 代理，本机 8081 网关这条路径也不可用。');
      console.log(`  可 docker restart ${container} 后重新执行上面的 start_container.sh。`);
    } else if (count > 1) {
      console.log('  容器里还有多个 server.lua，先清理到只剩一个：');
      console.log(`  docker exec ${container} sh -lc "pkill -f '[l]ua server.lua'"`);
    } else if (link) {
      console.log(`  宿主机 UDP 链路本身是通的（${used.host}:${cfg.port}），起完容器网关即可直接点②。`);
    }
  }
  console.log('');
}

main().catch((e) => { console.error(e); process.exit(1); });
