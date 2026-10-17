//=====================================================================
// test_gateway.js — 端到端联调自测（不需要 Docker 服务器）
//   在本地起一个"假 kfs 帧同步服务器"（UDP + 同款 ikcp.js，字节级同协议），
//   验证完整链路：WebSocket → gateway.js → UDP/KCP → 假服务器 → 广播回传。
//
// 运行前提：gateway.js 已用 KFS_SERVER_PORT=10999 WS_PORT=8082 启动。
//   node gateway.js 需配置 KFS_SERVER_PORT=10999 WS_PORT=8082
// 用法：node test_gateway.js
//=====================================================================

'use strict';

const dgram = require('dgram');
const { WebSocket } = require('ws');
const ikcp = require('./ikcp.js');

const MAGIC = 0x4B464346;
const FNV_OFFSET_BASIS = 2166136261;
const FNV_PRIME = 16777619;
const CMD = { INPUT: 1, BROADCAST: 2, READY: 7 };
const FRAME2_HEADER = 7;

const t0 = process.hrtime.bigint();
function now() { return Number((process.hrtime.bigint() - t0) / 1000000n) >>> 0; }
function fnv1a(buf) {
  let h = FNV_OFFSET_BASIS >>> 0;
  for (const b of buf) { h ^= b; h = Math.imul(h, FNV_PRIME) >>> 0; }
  return h;
}
function encodeFrame2(type, fid, data) {
  const out = Buffer.alloc(FRAME2_HEADER + data.length);
  out[0] = type;
  out.writeUInt32LE(fid >>> 0, 1);
  out.writeUInt16LE(data.length, 5);
  data.copy(out, FRAME2_HEADER);
  return out;
}
function decodeFrame2(pkt) {
  if (pkt.length < FRAME2_HEADER) return null;
  const type = pkt[0], fid = pkt.readUInt32LE(1), dlen = pkt.readUInt16LE(5);
  if (pkt.length !== FRAME2_HEADER + dlen) return null;
  return { type, fid, data: pkt.subarray(FRAME2_HEADER) };
}

//---------------------------------------------------------------------
// 假 kfs 服务器
//---------------------------------------------------------------------
const fake = dgram.createSocket('udp4');
const sessions = {};   // "ip:port" -> { kcp, conv, ready, fid, inputs }

function drive(s) {
  setInterval(() => {
    ikcp.update(s.kcp, now());
    // 推帧：只对已 ready 的会话，每 150ms 推一帧
    if (s.ready && now() - s.lastPush > 150) {
      s.lastPush = now();
      const fid = s.fid++;
      // 广播负载：逐玩家 [conv 4B][len 2B][data] + 尾 4B fnv1a(body)
      const parts = [];
      for (const k in s.inputs) {
        if (!s.inputs[k]) continue;
        const data = s.inputs[k][fid] || Buffer.from('0');
        const head = Buffer.alloc(6);
        head.writeUInt32LE(parseInt(k), 0);
        head.writeUInt16LE(data.length, 4);
        parts.push(head, data);
      }
      const body = Buffer.concat(parts);
      const hash = Buffer.alloc(4);
      hash.writeUInt32LE(fnv1a(body), 0);
      const payload = Buffer.concat([body, hash]);
      const frame = encodeFrame2(CMD.BROADCAST, fid, payload);
      ikcp.send(s.kcp, frame);
      console.log(`[fake] 推帧 fid=${fid} players=${Object.keys(s.inputs).length}`);
    }
  }, 5);
}

fake.on('message', (buf, rinfo) => {
  const key = rinfo.address + ':' + rinfo.port;
  if (buf.length >= 5 && buf.readUInt32LE(0) === MAGIC) {
    const room = buf.readUInt32LE(4);
    const conv = buf.readUInt32LE(8);
    const token = buf.readUInt32LE(12);
    console.log(`[fake] JOIN room=${room} conv=${conv} token=${token}`);
    const kcp = ikcp.create(conv, null);
    kcp.output = (b, len) => { fake.send(b.subarray(0, len), rinfo.port, rinfo.address); return len; };
    ikcp.nodelay(kcp, 1, 10, 2, 1);
    sessions[key] = { kcp, conv, ready: false, fid: 0, inputs: {}, lastPush: 0 };
    drive(sessions[key]);
    const ack = Buffer.alloc(5);
    ack.writeUInt32LE(MAGIC, 0); ack[4] = 0;
    fake.send(ack, rinfo.port, rinfo.address);
    return;
  }
  const s = sessions[key];
  if (!s) return;
  ikcp.input(s.kcp, buf);
  // 处理 KCP 会话内帧
  let raw;
  while ((raw = ikcp.recv(s.kcp)) !== null) {
    const f = decodeFrame2(raw);
    if (!f) continue;
    if (f.type === CMD.INPUT) {
      if (!s.inputs[s.conv]) s.inputs[s.conv] = {};
      s.inputs[s.conv][f.fid] = f.data;
      console.log(`[fake] INPUT conv=${s.conv} fid=${f.fid} data=${f.data.toString()}`);
    } else if (f.type === CMD.READY) {
      s.ready = true;
      console.log(`[fake] READY conv=${s.conv} → 开始推帧`);
    }
  }
});

fake.bind(10999, '127.0.0.1', () => console.log('[fake] 假 kfs 服务器 UDP 10999 已启动'));

//---------------------------------------------------------------------
// WS 客户端（模拟浏览器）
//---------------------------------------------------------------------
const ws = new WebSocket('ws://127.0.0.1:8082');
let joined = false, broadcasts = 0, checks = 0;

function send(op, obj) {
  ws.send(JSON.stringify(Object.assign({ op }, obj)));
}

ws.on('open', () => {
  console.log('[test] WS 已连接，发 JOIN...');
  send('join', { room_id: 100, conv: 1, token: 0, heartbeat_ms: 500 });
});

ws.on('message', (data) => {
  const msg = JSON.parse(data.toString());
  if (msg.op === 'joined') {
    joined = msg.ok;
    console.log(`[test] joined ok=${msg.ok} result=${msg.result}`);
    if (msg.ok) { send('ready'); send('input', { fid: 0, data: '2' }); send('input', { fid: 1, data: '2' }); }
  } else if (msg.op === 'broadcast') {
    broadcasts++;
    const payload = Buffer.from(msg.payload, 'hex');
    const fid = msg.fid;
    console.log(`[test] broadcast fid=${fid} 对账=${msg.ok} payloadLen=${payload.length}`);
    if (msg.ok) checks++;
    if (broadcasts >= 3) {
      const pass = joined && checks >= 3;
      console.log(pass ? '\n[test] ==== 端到端链路验证通过 ====' : '\n[test] ==== 链路异常 ====');
      console.log(`   joined=${joined} 对账通过帧数=${checks}/${broadcasts}`);
      process.exit(pass ? 0 : 1);
    }
  } else if (msg.op === 'log') {
    console.log(`[gw] ${msg.msg}`);
  }
});

setTimeout(() => { console.log('[test] 超时未完成'); process.exit(2); }, 15000);
