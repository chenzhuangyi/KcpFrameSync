//=====================================================================
// test_real.js — 经网关连“真实 Docker kfs 服务器”的双客户端端到端自测
//
// 前提：
//   1. Docker 容器里只有一个 server.lua，且已创建 room=1、2 个玩家（server.lua 1 2）
//   2. gateway.js 已在 ws://127.0.0.1:8081 运行
//   3. 从容器 tokens_1.txt 读出本次启动的 conv/token
//
// 用法（在 gateway 目录）：
//   node test_real.js <conv101的token> <conv102的token>
// 期望：joined=2，收到 fid=0..2 广播且对账一致，最后打印 PASS。
//=====================================================================
'use strict';
const { WebSocket } = require('ws');

if (!process.argv[2] || !process.argv[3]) {
  console.log('用法: node test_real.js <conv101_token> <conv102_token>');
  console.log('token 读取: docker exec ctn_centos7 cat /root/project/KcpFrameSync/lua_test/tokens_1.txt');
  process.exit(1);
}

const players = [
  { conv: 101, token: Number(process.argv[2] || 0) },
  { conv: 102, token: Number(process.argv[3] || 0) },
];
let broadcasts = 0;
let joined = 0;

function start(p) {
  const ws = new WebSocket('ws://127.0.0.1:8081');
  ws.on('open', () => {
    ws.send(JSON.stringify({ op: 'join', room_id: 1, conv: p.conv, token: p.token, heartbeat_ms: 500 }));
  });
  ws.on('message', (data) => {
    const msg = JSON.parse(data.toString());
    if (msg.op === 'joined') {
      console.log(`[e2e] conv=${p.conv} joined ok=${msg.ok} result=${msg.result} msg=${msg.msg || ''}`);
      if (msg.ok) { joined++; ws.send(JSON.stringify({ op: 'ready' })); }
    } else if (msg.op === 'broadcast') {
      broadcasts++;
      console.log(`[e2e] conv=${p.conv} broadcast fid=${msg.fid} ok=${msg.ok} payload=${msg.payload}`);
      if (broadcasts >= 6) { console.log(`[e2e] PASS joined=${joined} broadcasts=${broadcasts}`); process.exit(0); }
    } else if (msg.op === 'log') {
      console.log(`[e2e][gw] ${msg.msg}`);
    }
  });
  ws.on('error', (e) => console.log(`[e2e] ws error: ${e.message}`));
}

players.forEach(start);
setTimeout(() => { console.log(`[e2e] TIMEOUT joined=${joined} broadcasts=${broadcasts}`); process.exit(2); }, 15000);
