'use strict';
const dgram = require('dgram');
const ikcp = require('./ikcp.js');

const LOCAL_PORT = 10501;
const MAGIC = 0x4B464346;
const HS_LEN2 = 16;
const CMD = { INPUT: 1, BROADCAST: 2, READY: 7 };
const token = Number(process.argv[2] || 0);
const conv = 101;

const t0 = process.hrtime.bigint();
const now = () => Number((process.hrtime.bigint() - t0) / 1000000n) >>> 0;

function encode(type, fid, data) {
  const out = Buffer.alloc(7 + data.length);
  out[0] = type;
  out.writeUInt32LE(fid >>> 0, 1);
  out.writeUInt16LE(data.length, 5);
  data.copy(out, 7);
  return out;
}
function decode(pkt) {
  if (pkt.length < 7) return null;
  const type = pkt[0], fid = pkt.readUInt32LE(1), len = pkt.readUInt16LE(5);
  if (pkt.length !== 7 + len) return null;
  return { type, fid, data: pkt.subarray(7) };
}

const sock = dgram.createSocket('udp4');
let kcp = null, joined = false, frames = 0, sentInputs = false;

function sendUdp(buf) {
  sock.send(buf, 0, buf.length, LOCAL_PORT, '127.0.0.1');
}

sock.on('message', (buf) => {
  if (!joined && buf.length >= 5 && buf.readUInt32LE(0) === MAGIC) {
    joined = true;
    console.log(`[test] JOIN ACK result=${buf[4]}`);
    kcp = ikcp.create(conv, null);
    kcp.output = (b, len) => { sendUdp(b.subarray(0, len)); return len; };
    ikcp.nodelay(kcp, 1, 10, 2, 1);
    kcpSend(encode(CMD.READY, 0, Buffer.alloc(0)));
    for (let fid = 0; fid <= 2; fid++) kcpSend(encode(CMD.INPUT, fid, Buffer.from('4')));
    return;
  }
  if (kcp) ikcp.input(kcp, buf);
});

function kcpSend(frame) {
  const r = ikcp.send(kcp, frame);
  if (r !== 0) console.log('[test] ikcp.send failed', r);
}

const hs = Buffer.alloc(HS_LEN2);
hs.writeUInt32LE(MAGIC, 0);
hs.writeUInt32LE(1, 4);
hs.writeUInt32LE(conv, 8);
hs.writeUInt32LE(token, 12);
sock.bind(0, '127.0.0.1', () => {
  console.log(`[test] send JOIN to udp 127.0.0.1:${LOCAL_PORT} token=${token}`);
  sendUdp(hs);
});

setInterval(() => {
  if (!kcp) return;
  ikcp.update(kcp, now());
  let raw;
  while ((raw = ikcp.recv(kcp)) !== null) {
    const f = decode(raw);
    if (!f) continue;
    if (f.type !== CMD.BROADCAST) continue;
    frames++;
    console.log(`[test] BROADCAST fid=${f.fid} payload=${f.data.toString('hex')}`);
    if (!sentInputs) {
      sentInputs = true;
      // 服务器已经在推帧：按当前广播帧号补发方向 '4'（ASCII 0x34）
      for (let k = 1; k <= 3; k++) kcpSend(encode(CMD.INPUT, f.fid + k, Buffer.from('4')));
      console.log(`[test] sent INPUT fids ${f.fid + 1}..${f.fid + 3} data=4`);
    }
    if (frames >= 5) {
      console.log(`[test] PASS bridge UDP<->TCP KCP frames=${frames}`);
      process.exit(0);
    }
  }
}, 5);

setTimeout(() => { console.log(`[test] TIMEOUT joined=${joined} frames=${frames}`); process.exit(2); }, 12000);
