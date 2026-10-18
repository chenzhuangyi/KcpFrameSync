//=====================================================================
// bridge_windows.js — Windows 侧 UDP↔TCP 桥（给 Unity 用）
//
// 背景：Docker Desktop for Windows 的宿主机 UDP 端口代理会间歇性丢失，
//       Unity 直接 UDP 连 Docker 的 10500 会超时。本桥在 Windows 上监听
//       一个本地 UDP 端口（默认 10501），把每个 Unity 客户端（按源 ip:port）
//       映射到一条 TCP 连接，连到容器内 bridge_container.js 的 10500/tcp。
//
// 链路：
//   Unity --UDP 127.0.0.1:10501--> 本桥 --TCP 127.0.0.1:10500--> 容器内桥
//         --UDP 127.0.0.1:10500--> server.lua
//
// Unity 只需把 kfs 客户端的 setpeer 端口从 10500 改成 10501。
//
// 用法（在 gateway 目录）：
//   node bridge_windows.js
// 环境变量：
//   BRIDGE_LOCAL_HOST  默认 127.0.0.1
//   BRIDGE_LOCAL_PORT  默认 10501（Unity 连这个）
//   BRIDGE_REMOTE_HOST 默认 127.0.0.1
//   BRIDGE_REMOTE_PORT 默认 10500（Docker 发布的 TCP 10500）
//   BRIDGE_IDLE_MS     默认 300000
//=====================================================================
'use strict';

const dgram = require('dgram');
const net = require('net');

const LOCAL_HOST = process.env.BRIDGE_LOCAL_HOST || '127.0.0.1';
const LOCAL_PORT = parseInt(process.env.BRIDGE_LOCAL_PORT || '10501', 10);
const REMOTE_HOST = process.env.BRIDGE_REMOTE_HOST || '127.0.0.1';
const REMOTE_PORT = parseInt(process.env.BRIDGE_REMOTE_PORT || '10500', 10);
const IDLE_MS = parseInt(process.env.BRIDGE_IDLE_MS || '300000', 10);

function frame(payload) {
  const out = Buffer.allocUnsafe(2 + payload.length);
  out.writeUInt16BE(payload.length, 0);
  payload.copy(out, 2);
  return out;
}

function attachParser(sock, onPayload) {
  let buf = Buffer.alloc(0);
  sock.on('data', (chunk) => {
    buf = buf.length ? Buffer.concat([buf, chunk]) : chunk;
    while (buf.length >= 2) {
      const len = buf.readUInt16BE(0);
      if (buf.length < 2 + len) break;
      const payload = Buffer.from(buf.subarray(2, 2 + len));
      buf = buf.subarray(2 + len);
      onPayload(payload);
    }
  });
}

const udp = dgram.createSocket('udp4');
const clients = new Map(); // "ip:port" -> { rinfo, tcp, connected, queue, lastActive }

function connect(c) {
  const tcp = net.connect({ host: REMOTE_HOST, port: REMOTE_PORT });
  c.tcp = tcp;
  tcp.setNoDelay(true);

  tcp.on('connect', () => {
    c.connected = true;
    console.log(`[bridge] ${c.key} -> tcp ${REMOTE_HOST}:${REMOTE_PORT}`);
    for (const p of c.queue) tcp.write(frame(p));
    c.queue.length = 0;
  });

  attachParser(tcp, (payload) => {
    c.lastActive = Date.now();
    udp.send(payload, c.rinfo.port, c.rinfo.address, (err) => {
      if (err) console.log(`[bridge] udp reply error ${c.key}: ${err.message}`);
    });
  });

  tcp.on('error', (e) => {
    console.log(`[bridge] tcp error ${c.key}: ${e.message}`);
  });
  tcp.on('close', () => {
    c.connected = false;
    if (clients.get(c.key) === c) clients.delete(c.key);
    console.log(`[bridge] tcp closed ${c.key}`);
  });
}

function getClient(rinfo) {
  const key = `${rinfo.address}:${rinfo.port}`;
  let c = clients.get(key);
  if (c) return c;
  c = { key, rinfo, tcp: null, connected: false, queue: [], lastActive: Date.now() };
  clients.set(key, c);
  console.log(`[bridge] new udp client ${key}`);
  connect(c);
  return c;
}

udp.on('message', (msg, rinfo) => {
  const c = getClient(rinfo);
  c.lastActive = Date.now();
  if (c.connected && c.tcp && !c.tcp.destroyed) {
    c.tcp.write(frame(msg));
  } else {
    if (c.queue.length < 200) c.queue.push(Buffer.from(msg));
  }
});

udp.on('error', (e) => {
  console.error(`[bridge] udp listen error: ${e.message}`);
  process.exit(1);
});

udp.bind(LOCAL_PORT, LOCAL_HOST, () => {
  console.log(`[bridge] windows bridge udp ${LOCAL_HOST}:${LOCAL_PORT} -> tcp ${REMOTE_HOST}:${REMOTE_PORT}`);
  console.log(`[bridge] Unity kfs 客户端 setpeer("${LOCAL_HOST}", ${LOCAL_PORT})`);
});

setInterval(() => {
  const now = Date.now();
  for (const c of clients.values()) {
    if (now - c.lastActive > IDLE_MS) {
      console.log(`[bridge] idle close ${c.key}`);
      if (c.tcp) c.tcp.destroy();
      clients.delete(c.key);
    }
  }
}, 30000);

function shutdown() {
  for (const c of clients.values()) {
    if (c.tcp) c.tcp.destroy();
  }
  try { udp.close(); } catch (e) { /* ignore */ }
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
