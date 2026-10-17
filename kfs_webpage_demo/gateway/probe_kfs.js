//=====================================================================
// probe_kfs.js — 探测 kfs 帧同步服务器是否在运行、握手是否通
//   向 KFS_SERVER_HOST:PORT 发一个裸 UDP JOIN 握手包（room_id/conv/token 任意），
//   等 3 秒看有没有 5 字节应答。
//   用法：node probe_kfs.js [host] [port] [room_id] [conv] [token]
//   默认：node probe_kfs.js   → 127.0.0.1:10500 room=1 conv=999 token=0
//
//   结果判读：
//     "收到应答 result=0..5" → 服务器在跑、握手链路通（网关连不上请查网关）
//     "无应答(3s超时)"       → 服务器没跑/端口不通 → 去 Docker 把服务器跑起来
//=====================================================================
'use strict';
const dgram = require('dgram');

const host = process.argv[2] || '127.0.0.1';
const port = parseInt(process.argv[3] || '10500', 10);
const room = parseInt(process.argv[4] || '1', 10) >>> 0;
const conv = parseInt(process.argv[5] || '999', 10) >>> 0;
const token = parseInt(process.argv[6] || '0', 10) >>> 0;

const MAGIC = 0x4B464346;
const sock = dgram.createSocket('udp4');
let answered = false;

const hs = Buffer.alloc(16);
hs.writeUInt32LE(MAGIC, 0);   // 小端（与 kfs_core.c memcpy 一致）
hs.writeUInt32LE(room, 4);
hs.writeUInt32LE(conv, 8);
hs.writeUInt32LE(token, 12);

sock.on('message', (buf) => {
  if (buf.length >= 5 && buf.readUInt32LE(0) === MAGIC) {
    answered = true;
    const r = buf[4];
    const names = ['成功','房间不存在','房间已满','conv冲突','其他错误','token错误'];
    console.log(`\n>>> 收到应答 result=${r} (${names[r] || '未知'})  ← 服务器在跑、握手链路通`);
    console.log(`    应答原始: ${buf.toString('hex')}`);
  } else {
    console.log(`收到非握手包(长度${buf.length}): ${buf.toString('hex')}`);
  }
  sock.close();
  process.exit(0);
});

sock.bind(0, () => {
  sock.send(hs, 0, 16, port, host, (err) => {
    if (err) { console.log(`发送失败: ${err.message}`); sock.close(); process.exit(1); }
    console.log(`已发送 JOIN 握手到 ${host}:${port}  room=${room} conv=${conv} token=${token}`);
    console.log(`包体: ${hs.toString('hex')}`);
  });
});

setTimeout(() => {
  if (!answered) {
    console.log(`\n>>> 3 秒无应答 → 服务器没在跑/端口不通`);
    console.log(`    请先在 Docker 里启动 kfs 服务器并确认 UDP ${port} 可访问，再重试。`);
    sock.close();
    process.exit(1);
  }
}, 3000);
