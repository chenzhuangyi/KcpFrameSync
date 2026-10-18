//=====================================================================
// bridge_container.js — 容器内 TCP↔UDP 桥（给 Windows 上的 Unity 用）
//
// 背景：Docker Desktop for Windows 的宿主机 UDP 端口代理（gVisor/VPNKit）
//       会间歇性丢失或只绑 IPv6，Windows 进程直接 UDP 连 10500 不可靠。
//       本桥在容器内监听 TCP 10500（Docker 已发布且稳定），把每个 TCP 连接
//       映射成一个独立的 UDP socket，转发到 127.0.0.1:10500 的 kfs 服务器。
//
// 链路：
//   Unity/Windows 桥 --TCP 10500--> 本桥 --UDP 127.0.0.1:10500--> server.lua
//
// 帧格式：[len 2B big-endian][payload]，一个 payload 即一个 UDP 数据报。
//
// 启动（容器内）：
//   /usr/local/bin/kfs-node bridge_container.js
// 环境变量：
//   BRIDGE_TCP_PORT    默认 10500（容器内监听）
//   BRIDGE_SERVER_HOST 默认 127.0.0.1
//   BRIDGE_SERVER_PORT 默认 10500
//   BRIDGE_IDLE_MS     默认 120000（无流量多久关闭连接）
//=====================================================================
'use strict';

const net = require('net');
const dgram = require('dgram');

const TCP_PORT = parseInt(process.env.BRIDGE_TCP_PORT || '10500', 10);
const SERVER_HOST = process.env.BRIDGE_SERVER_HOST || '127.0.0.1';
const SERVER_PORT = parseInt(process.env.BRIDGE_SERVER_PORT || '10500', 10);
const IDLE_MS = parseInt(process.env.BRIDGE_IDLE_MS || '120000', 10);

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

const server = net.createServer((tcp) => {
  const peer = `${tcp.remoteAddress}:${tcp.remotePort}`;
  const udp = dgram.createSocket('udp4');
  let closed = false;
  let lastActive = Date.now();
  let udpPort = 0;

  const close = (why) => {
    if (closed) return;
    closed = true;
    clearInterval(timer);
    try { tcp.destroy(); } catch (e) { /* ignore */ }
    try { udp.close(); } catch (e) { /* ignore */ }
    console.log(`[bridge] close ${peer} udp=${udpPort} (${why})`);
  };

  udp.on('message', (msg) => {
    lastActive = Date.now();
    if (!tcp.destroyed) tcp.write(frame(msg));
  });
  udp.on('error', (e) => close(`udp: ${e.message}`));
  udp.bind(0, '127.0.0.1', () => {
    udpPort = udp.address().port;
    console.log(`[bridge] open ${peer} -> udp 127.0.0.1:${udpPort}`);
  });

  attachParser(tcp, (payload) => {
    lastActive = Date.now();
    udp.send(payload, SERVER_PORT, SERVER_HOST, (err) => {
      if (err) console.log(`[bridge] udp send error ${peer}: ${err.message}`);
    });
  });

  tcp.on('error', (e) => close(`tcp: ${e.message}`));
  tcp.on('close', () => close('tcp closed'));
  tcp.on('end', () => close('tcp end'));

  const timer = setInterval(() => {
    if (Date.now() - lastActive > IDLE_MS) close('idle');
  }, 30000);
});

server.on('error', (e) => {
  console.error(`[bridge] listen error: ${e.message}`);
  process.exit(1);
});

server.listen(TCP_PORT, '0.0.0.0', () => {
  console.log(`[bridge] container bridge listening tcp/${TCP_PORT} -> udp ${SERVER_HOST}:${SERVER_PORT}`);
});

process.on('SIGINT', () => { server.close(); process.exit(0); });
process.on('SIGTERM', () => { server.close(); process.exit(0); });
