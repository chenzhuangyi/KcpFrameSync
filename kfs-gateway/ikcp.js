//=====================================================================
// ikcp.js — KCP - A Better ARQ Protocol Implementation
// skywind3000 (at) gmail.com, 2010-2011
//
// 本文件是把 kfs_src/third/kcp/ikcp.c 逐行转写的纯 JS 版本（Node 模块），
// 与 C 版字节级兼容（conv/sn/una/cmd 格式一致），供 WebSocket→UDP 网关使用。
// 所有 32 位无符号运算按 C 语义处理（>>>0 / |0 / Math.imul）。
// API 与 ikcp.h 一致：create/release/setoutput/recv/send/update/check/
//   input/flush/peeksize/setmtu/wndsize/waitsnd/nodelay/interval/getconv
//=====================================================================

'use strict';

const IKCP_RTO_NDL = 30;
const IKCP_RTO_MIN = 100;
const IKCP_RTO_DEF = 200;
const IKCP_RTO_MAX = 60000;
const IKCP_CMD_PUSH = 81;
const IKCP_CMD_ACK  = 82;
const IKCP_CMD_WASK = 83;
const IKCP_CMD_WINS = 84;
const IKCP_ASK_SEND = 1;
const IKCP_ASK_TELL = 2;
const IKCP_WND_SND  = 32;
const IKCP_WND_RCV  = 128;
const IKCP_MTU_DEF  = 1400;
const IKCP_ACK_FAST = 3;
const IKCP_INTERVAL = 100;
const IKCP_OVERHEAD = 24;
const IKCP_DEADLINK = 20;
const IKCP_THRESH_INIT = 2;
const IKCP_THRESH_MIN = 2;
const IKCP_PROBE_INIT = 7000;
const IKCP_PROBE_LIMIT = 120000;
const IKCP_FASTACK_LIMIT = 5;

// 32 位有符号差（对应 (IINT32)(later - earlier)）
function itimediff(later, earlier) { return (later - earlier) | 0; }
function imin(a, b) { return a <= b ? a : b; }
function imax(a, b) { return a >= b ? a : b; }
function ibound(lower, middle, upper) { return imin(imax(lower, middle), upper); }

// 创建 KCP 控制对象（对应 ikcp_create）
function create(conv, user) {
  const kcp = {
    conv: conv >>> 0,
    user: user,
    mtu: IKCP_MTU_DEF,
    mss: IKCP_MTU_DEF - IKCP_OVERHEAD,
    state: 0,
    snd_una: 0, snd_nxt: 0, rcv_nxt: 0,
    ts_recent: 0, ts_lastack: 0, ssthresh: IKCP_THRESH_INIT,
    rx_rttval: 0, rx_srtt: 0, rx_rto: IKCP_RTO_DEF, rx_minrto: IKCP_RTO_MIN,
    snd_wnd: IKCP_WND_SND, rcv_wnd: IKCP_WND_RCV, rmt_wnd: IKCP_WND_RCV,
    cwnd: 0, incr: 0, probe: 0,
    current: 0, interval: IKCP_INTERVAL, ts_flush: IKCP_INTERVAL,
    xmit: 0, nrcv_buf: 0, nsnd_buf: 0, nrcv_que: 0, nsnd_que: 0,
    nodelay: 0, updated: 0, ts_probe: 0, probe_wait: 0,
    dead_link: IKCP_DEADLINK,
    stream: 0,
    snd_queue: [],   // FIFO
    rcv_queue: [],   // FIFO
    snd_buf: [],     // 按 sn 升序
    rcv_buf: [],     // 按 sn 升序
    acklist: [],     // 平铺 [sn, ts, ...]
    ackcount: 0,
    buffer: Buffer.alloc((IKCP_MTU_DEF + IKCP_OVERHEAD) * 3),
    fastresend: 0, fastlimit: IKCP_FASTACK_LIMIT, nocwnd: 0,
    logmask: 0,
    output: null,
    writelog: null,
  };
  return kcp;
}

// 释放 KCP 对象（对应 ikcp_release；JS 无手动内存，清队列即可）
function release(kcp) {
  if (!kcp) return;
  kcp.snd_buf.length = 0;
  kcp.rcv_buf.length = 0;
  kcp.snd_queue.length = 0;
  kcp.rcv_queue.length = 0;
  kcp.acklist.length = 0;
  kcp.ackcount = 0;
  kcp.nrcv_buf = 0; kcp.nsnd_buf = 0; kcp.nrcv_que = 0; kcp.nsnd_que = 0;
}

function setoutput(kcp, fn) { kcp.output = fn; }

function output(kcp, data, size) {
  if (size === 0) return 0;
  if (!kcp.output) return 0;
  return kcp.output(data, size, kcp, kcp.user);
}

// 下一帧完整消息长度（对应 ikcp_peeksize；-1 无）
function peeksize(kcp) {
  if (kcp.rcv_queue.length === 0) return -1;
  const seg = kcp.rcv_queue[0];
  if (seg.frg === 0) return seg.len;
  if (kcp.nrcv_que < seg.frg + 1) return -1;
  let length = 0;
  for (const s of kcp.rcv_queue) {
    length += s.len;
    if (s.frg === 0) break;
  }
  return length;
}

// 接收一条完整消息（对应 ikcp_recv）。无数据返回 null；否则返回 Buffer。
function recv(kcp) {
  if (kcp.rcv_queue.length === 0) return null;
  const psize = peeksize(kcp);
  if (psize < 0) return null;

  let recover = 0;
  if (kcp.nrcv_que >= kcp.rcv_wnd) recover = 1;

  // 合并 fragment
  const out = Buffer.alloc(psize);
  let off = 0;
  while (kcp.rcv_queue.length > 0) {
    const seg = kcp.rcv_queue.shift();
    seg.data.copy(out, off, 0, seg.len);
    off += seg.len;
    kcp.nrcv_que = (kcp.nrcv_que - 1) >>> 0;
    if (seg.frg === 0) break;
  }

  // rcv_buf -> rcv_queue
  while (kcp.rcv_buf.length > 0) {
    const seg = kcp.rcv_buf[0];
    if (seg.sn === kcp.rcv_nxt && kcp.nrcv_que < kcp.rcv_wnd) {
      kcp.rcv_buf.shift();
      kcp.nrcv_buf = (kcp.nrcv_buf - 1) >>> 0;
      kcp.rcv_queue.push(seg);
      kcp.nrcv_que = (kcp.nrcv_que + 1) >>> 0;
      kcp.rcv_nxt = (kcp.rcv_nxt + 1) >>> 0;
    } else break;
  }

  // fast recover
  if (kcp.nrcv_que < kcp.rcv_wnd && recover) {
    kcp.probe = (kcp.probe | IKCP_ASK_TELL) >>> 0;
  }
  return out;
}

function update_ack(kcp, rtt) {
  let rto = 0;
  if (kcp.rx_srtt === 0) {
    kcp.rx_srtt = rtt;
    kcp.rx_rttval = (rtt / 2) | 0;
  } else {
    let delta = rtt - kcp.rx_srtt;
    if (delta < 0) delta = -delta;
    kcp.rx_rttval = (((3 * kcp.rx_rttval + delta) / 4) | 0) >>> 0;
    kcp.rx_srtt = (((7 * kcp.rx_srtt + rtt) / 8) | 0) >>> 0;
    if (kcp.rx_srtt < 1) kcp.rx_srtt = 1;
  }
  rto = (kcp.rx_srtt + imax(kcp.interval, 4 * kcp.rx_rttval)) >>> 0;
  kcp.rx_rto = ibound(kcp.rx_minrto, rto, IKCP_RTO_MAX);
}

function shrink_buf(kcp) {
  if (kcp.snd_buf.length > 0) kcp.snd_una = kcp.snd_buf[0].sn;
  else kcp.snd_una = kcp.snd_nxt;
}

function parse_ack(kcp, sn) {
  if (itimediff(sn, kcp.snd_una) < 0 || itimediff(sn, kcp.snd_nxt) >= 0) return;
  for (let i = 0; i < kcp.snd_buf.length; i++) {
    const seg = kcp.snd_buf[i];
    if (sn === seg.sn) {
      kcp.snd_buf.splice(i, 1);
      kcp.nsnd_buf = (kcp.nsnd_buf - 1) >>> 0;
      break;
    }
    if (itimediff(sn, seg.sn) < 0) break;
  }
}

function parse_una(kcp, una) {
  while (kcp.snd_buf.length > 0) {
    const seg = kcp.snd_buf[0];
    if (itimediff(una, seg.sn) > 0) {
      kcp.snd_buf.shift();
      kcp.nsnd_buf = (kcp.nsnd_buf - 1) >>> 0;
    } else break;
  }
}

function parse_fastack(kcp, sn, ts) {
  if (itimediff(sn, kcp.snd_una) < 0 || itimediff(sn, kcp.snd_nxt) >= 0) return;
  for (const seg of kcp.snd_buf) {
    if (itimediff(sn, seg.sn) < 0) break;
    else if (sn !== seg.sn) seg.fastack = (seg.fastack + 1) >>> 0;
  }
}

function ack_push(kcp, sn, ts) {
  kcp.acklist.push(sn >>> 0, ts >>> 0);
  kcp.ackcount = (kcp.ackcount + 1) >>> 0;
}
function ack_get(kcp, p) {
  return { sn: kcp.acklist[p * 2], ts: kcp.acklist[p * 2 + 1] };
}

function parse_data(kcp, newseg) {
  const sn = newseg.sn;
  if (itimediff(sn, (kcp.rcv_nxt + kcp.rcv_wnd) >>> 0) >= 0 || itimediff(sn, kcp.rcv_nxt) < 0) {
    return; // 丢弃
  }
  let repeat = false, insertAt = 0;
  for (let i = kcp.rcv_buf.length - 1; i >= 0; i--) {
    const seg = kcp.rcv_buf[i];
    if (seg.sn === sn) { repeat = true; break; }
    if (itimediff(sn, seg.sn) > 0) { insertAt = i + 1; break; }
  }
  if (!repeat) {
    kcp.rcv_buf.splice(insertAt, 0, newseg);
    kcp.nrcv_buf = (kcp.nrcv_buf + 1) >>> 0;
  }
  // rcv_buf -> rcv_queue
  while (kcp.rcv_buf.length > 0) {
    const seg = kcp.rcv_buf[0];
    if (seg.sn === kcp.rcv_nxt && kcp.nrcv_que < kcp.rcv_wnd) {
      kcp.rcv_buf.shift();
      kcp.nrcv_buf = (kcp.nrcv_buf - 1) >>> 0;
      kcp.rcv_queue.push(seg);
      kcp.nrcv_que = (kcp.nrcv_que + 1) >>> 0;
      kcp.rcv_nxt = (kcp.rcv_nxt + 1) >>> 0;
    } else break;
  }
}

// 输入底层包（对应 ikcp_input）
function input(kcp, data) {
  if (!data || data.length < IKCP_OVERHEAD) return -1;
  const size = data.length;
  const prev_una = kcp.snd_una;
  let maxack = 0, latest_ts = 0, flag = 0;
  let off = 0;

  while (true) {
    if (size - off < IKCP_OVERHEAD) break;
    const conv = data.readUInt32LE(off); off += 4;
    if (conv !== kcp.conv) return -1;
    const cmd = data[off++];
    const frg = data[off++];
    const wnd = data.readUInt16LE(off); off += 2;
    const ts  = data.readUInt32LE(off); off += 4;
    const sn  = data.readUInt32LE(off); off += 4;
    const una = data.readUInt32LE(off); off += 4;
    const len = data.readUInt32LE(off); off += 4;
    if (size - off < len) return -2;
    if (cmd !== IKCP_CMD_PUSH && cmd !== IKCP_CMD_ACK &&
        cmd !== IKCP_CMD_WASK && cmd !== IKCP_CMD_WINS) return -3;

    kcp.rmt_wnd = wnd;
    parse_una(kcp, una);
    shrink_buf(kcp);

    if (cmd === IKCP_CMD_ACK) {
      if (itimediff(kcp.current, ts) >= 0) update_ack(kcp, itimediff(kcp.current, ts));
      parse_ack(kcp, sn);
      shrink_buf(kcp);
      if (flag === 0) { flag = 1; maxack = sn; latest_ts = ts; }
      else if (itimediff(sn, maxack) > 0) { maxack = sn; latest_ts = ts; }
    } else if (cmd === IKCP_CMD_PUSH) {
      if (itimediff(sn, (kcp.rcv_nxt + kcp.rcv_wnd) >>> 0) < 0) {
        ack_push(kcp, sn, ts);
        if (itimediff(sn, kcp.rcv_nxt) >= 0) {
          const seg = {
            conv: conv, cmd: cmd, frg: frg, wnd: wnd, ts: ts, sn: sn, una: una,
            len: len, resendts: 0, rto: 0, fastack: 0, xmit: 0,
            data: Buffer.from(data.subarray(off, off + len)),
          };
          parse_data(kcp, seg);
        }
      }
    } else if (cmd === IKCP_CMD_WASK) {
      kcp.probe = (kcp.probe | IKCP_ASK_TELL) >>> 0;
    } else if (cmd === IKCP_CMD_WINS) {
      // nothing
    } else {
      return -3;
    }
    off += len;
  }

  if (flag !== 0) parse_fastack(kcp, maxack, latest_ts);

  if (itimediff(kcp.snd_una, prev_una) > 0) {
    if (kcp.cwnd < kcp.rmt_wnd) {
      const mss = kcp.mss;
      if (kcp.cwnd < kcp.ssthresh) {
        kcp.cwnd = (kcp.cwnd + 1) >>> 0;
        kcp.incr = (kcp.incr + mss) >>> 0;
      } else {
        if (kcp.incr < mss) kcp.incr = mss;
        kcp.incr = (kcp.incr + (((Math.imul(mss, mss) >>> 0) / kcp.incr | 0) + ((mss / 16) | 0))) >>> 0;
        if (((kcp.cwnd + 1) * mss) <= kcp.incr) {
          kcp.cwnd = (((kcp.incr + mss - 1) / (mss > 0 ? mss : 1)) | 0) >>> 0;
        }
      }
      if (kcp.cwnd > kcp.rmt_wnd) {
        kcp.cwnd = kcp.rmt_wnd;
        kcp.incr = Math.imul(kcp.rmt_wnd, mss) >>> 0;
      }
    }
  }
  return 0;
}

function encode_seg(buf, off, seg) {
  buf.writeUInt32LE(seg.conv >>> 0, off); off += 4;
  buf[off++] = seg.cmd & 0xff;
  buf[off++] = seg.frg & 0xff;
  buf.writeUInt16LE(seg.wnd & 0xffff, off); off += 2;
  buf.writeUInt32LE(seg.ts >>> 0, off); off += 4;
  buf.writeUInt32LE(seg.sn >>> 0, off); off += 4;
  buf.writeUInt32LE(seg.una >>> 0, off); off += 4;
  buf.writeUInt32LE(seg.len >>> 0, off); off += 4;
  return off;
}

function wnd_unused(kcp) {
  if (kcp.nrcv_que < kcp.rcv_wnd) return (kcp.rcv_wnd - kcp.nrcv_que) >>> 0;
  return 0;
}

// 发送缓冲区（对应 ikcp_flush）
function flush(kcp) {
  if (kcp.updated === 0) return;
  const buffer = kcp.buffer;
  let ptr = 0;

  const seg = {
    conv: kcp.conv, cmd: IKCP_CMD_ACK, frg: 0, wnd: wnd_unused(kcp),
    una: kcp.rcv_nxt, len: 0, sn: 0, ts: 0,
  };

  // ack
  const count = kcp.ackcount;
  for (let i = 0; i < count; i++) {
    const size = ptr;
    if (size + IKCP_OVERHEAD > kcp.mtu) { output(kcp, buffer.subarray(0, size), size); ptr = 0; }
    const a = ack_get(kcp, i);
    seg.sn = a.sn; seg.ts = a.ts;
    ptr = encode_seg(buffer, ptr, seg);
  }
  kcp.ackcount = 0;

  // probe window
  if (kcp.rmt_wnd === 0) {
    if (kcp.probe_wait === 0) {
      kcp.probe_wait = IKCP_PROBE_INIT;
      kcp.ts_probe = (kcp.current + kcp.probe_wait) >>> 0;
    } else {
      if (itimediff(kcp.current, kcp.ts_probe) >= 0) {
        if (kcp.probe_wait < IKCP_PROBE_INIT) kcp.probe_wait = IKCP_PROBE_INIT;
        kcp.probe_wait = (kcp.probe_wait + (kcp.probe_wait / 2 | 0)) >>> 0;
        if (kcp.probe_wait > IKCP_PROBE_LIMIT) kcp.probe_wait = IKCP_PROBE_LIMIT;
        kcp.ts_probe = (kcp.current + kcp.probe_wait) >>> 0;
        kcp.probe = (kcp.probe | IKCP_ASK_SEND) >>> 0;
      }
    }
  } else {
    kcp.ts_probe = 0;
    kcp.probe_wait = 0;
  }

  if (kcp.probe & IKCP_ASK_SEND) {
    seg.cmd = IKCP_CMD_WASK;
    const size = ptr;
    if (size + IKCP_OVERHEAD > kcp.mtu) { output(kcp, buffer.subarray(0, size), size); ptr = 0; }
    ptr = encode_seg(buffer, ptr, seg);
  }
  if (kcp.probe & IKCP_ASK_TELL) {
    seg.cmd = IKCP_CMD_WINS;
    const size = ptr;
    if (size + IKCP_OVERHEAD > kcp.mtu) { output(kcp, buffer.subarray(0, size), size); ptr = 0; }
    ptr = encode_seg(buffer, ptr, seg);
  }
  kcp.probe = 0;

  // window
  let cwnd = imin(kcp.snd_wnd, kcp.rmt_wnd);
  if (kcp.nocwnd === 0) cwnd = imin(kcp.cwnd, cwnd);

  // snd_queue -> snd_buf
  while (itimediff(kcp.snd_nxt, (kcp.snd_una + cwnd) >>> 0) < 0) {
    if (kcp.snd_queue.length === 0) break;
    const newseg = kcp.snd_queue.shift();
    kcp.nsnd_que = (kcp.nsnd_que - 1) >>> 0;
    kcp.snd_buf.push(newseg);
    kcp.nsnd_buf = (kcp.nsnd_buf + 1) >>> 0;
    newseg.conv = kcp.conv;
    newseg.cmd = IKCP_CMD_PUSH;
    newseg.wnd = seg.wnd;
    newseg.ts = kcp.current;
    newseg.sn = kcp.snd_nxt;
    kcp.snd_nxt = (kcp.snd_nxt + 1) >>> 0;
    newseg.una = kcp.rcv_nxt;
    newseg.resendts = kcp.current;
    newseg.rto = kcp.rx_rto;
    newseg.fastack = 0;
    newseg.xmit = 0;
  }

  const resent = (kcp.fastresend > 0) ? kcp.fastresend : 0xffffffff;
  const rtomin = (kcp.nodelay === 0) ? (kcp.rx_rto >> 3) : 0;

  let change = 0, lost = 0;
  // flush data segments
  for (let i = 0; i < kcp.snd_buf.length; i++) {
    const segment = kcp.snd_buf[i];
    let needsend = 0;
    if (segment.xmit === 0) {
      needsend = 1;
      segment.xmit = (segment.xmit + 1) >>> 0;
      segment.rto = kcp.rx_rto;
      segment.resendts = (kcp.current + segment.rto + rtomin) >>> 0;
    } else if (itimediff(kcp.current, segment.resendts) >= 0) {
      needsend = 1;
      segment.xmit = (segment.xmit + 1) >>> 0;
      kcp.xmit = (kcp.xmit + 1) >>> 0;
      if (kcp.nodelay === 0) {
        segment.rto = (segment.rto + imax(segment.rto, kcp.rx_rto)) >>> 0;
      } else {
        const step = (kcp.nodelay < 2) ? segment.rto : kcp.rx_rto;
        segment.rto = (segment.rto + (step / 2 | 0)) >>> 0;
      }
      segment.resendts = (kcp.current + segment.rto) >>> 0;
      lost = 1;
    } else if (segment.fastack >= resent) {
      if (segment.xmit <= kcp.fastlimit || kcp.fastlimit <= 0) {
        needsend = 1;
        segment.xmit = (segment.xmit + 1) >>> 0;
        segment.fastack = 0;
        segment.resendts = (kcp.current + segment.rto) >>> 0;
        change++;
      }
    }

    if (needsend) {
      segment.ts = kcp.current;
      segment.wnd = seg.wnd;
      segment.una = kcp.rcv_nxt;

      const size = ptr;
      const need = IKCP_OVERHEAD + segment.len;
      if (size + need > kcp.mtu) { output(kcp, buffer.subarray(0, size), size); ptr = 0; }
      ptr = encode_seg(buffer, ptr, segment);
      if (segment.len > 0) {
        segment.data.copy(buffer, ptr, 0, segment.len);
        ptr += segment.len;
      }
      if (segment.xmit >= kcp.dead_link) kcp.state = 0xffffffff; // -1
    }
  }

  if (ptr > 0) output(kcp, buffer.subarray(0, ptr), ptr);

  if (change) {
    const inflight = (kcp.snd_nxt - kcp.snd_una) >>> 0;
    kcp.ssthresh = (inflight / 2 | 0) >>> 0;
    if (kcp.ssthresh < IKCP_THRESH_MIN) kcp.ssthresh = IKCP_THRESH_MIN;
    kcp.cwnd = (kcp.ssthresh + resent) >>> 0;
    kcp.incr = Math.imul(kcp.cwnd, kcp.mss) >>> 0;
  }

  if (lost) {
    kcp.ssthresh = (cwnd / 2 | 0) >>> 0;
    if (kcp.ssthresh < IKCP_THRESH_MIN) kcp.ssthresh = IKCP_THRESH_MIN;
    kcp.cwnd = 1;
    kcp.incr = kcp.mss;
  }

  if (kcp.cwnd < 1) { kcp.cwnd = 1; kcp.incr = kcp.mss; }
}

// 驱动更新（对应 ikcp_update；current 为相对毫秒）
function update(kcp, current) {
  kcp.current = current >>> 0;
  if (kcp.updated === 0) { kcp.updated = 1; kcp.ts_flush = kcp.current; }
  let slap = itimediff(kcp.current, kcp.ts_flush);
  if (slap >= 10000 || slap < -10000) { kcp.ts_flush = kcp.current; slap = 0; }
  if (slap >= 0) {
    kcp.ts_flush = (kcp.ts_flush + kcp.interval) >>> 0;
    if (itimediff(kcp.current, kcp.ts_flush) >= 0) {
      kcp.ts_flush = (kcp.current + kcp.interval) >>> 0;
    }
    flush(kcp);
  }
}

function check(kcp, current) {
  let ts_flush = kcp.ts_flush;
  let tm_flush = 0x7fffffff;
  let tm_packet = 0x7fffffff;
  let minimal = 0;
  if (kcp.updated === 0) return current >>> 0;
  if (itimediff(current, ts_flush) >= 10000 || itimediff(current, ts_flush) < -10000) {
    ts_flush = current;
  }
  if (itimediff(current, ts_flush) >= 0) return current >>> 0;
  tm_flush = itimediff(ts_flush, current);
  for (const seg of kcp.snd_buf) {
    const diff = itimediff(seg.resendts, current);
    if (diff <= 0) return current >>> 0;
    if (diff < tm_packet) tm_packet = diff;
  }
  minimal = (tm_packet < tm_flush ? tm_packet : tm_flush) >>> 0;
  if (minimal >= kcp.interval) minimal = kcp.interval;
  return (current + minimal) >>> 0;
}

// 发送一条消息（对应 ikcp_send）
function send(kcp, buffer) {
  let len = buffer.length;
  if (kcp.mss <= 0) return -1;
  if (len < 0) return -1;

  if (kcp.stream !== 0 && kcp.snd_queue.length > 0) {
    const old = kcp.snd_queue[kcp.snd_queue.length - 1];
    if (old.len < kcp.mss) {
      const capacity = kcp.mss - old.len;
      const extend = (len < capacity) ? len : capacity;
      const seg = {
        conv: 0, cmd: 0, frg: 0, wnd: 0, ts: 0, sn: 0, una: 0,
        len: old.len + extend, resendts: 0, rto: 0, fastack: 0, xmit: 0,
        data: Buffer.concat([old.data, buffer.subarray(0, extend)]),
      };
      kcp.snd_queue.pop();
      kcp.snd_queue.push(seg);
      buffer = buffer.subarray(extend);
      len -= extend;
      if (len <= 0) return 0;
    }
  }

  let count = (len <= kcp.mss) ? 1 : Math.ceil(len / kcp.mss);
  if (count >= IKCP_WND_RCV) return -2;
  if (count === 0) count = 1;

  for (let i = 0; i < count; i++) {
    const size = len > kcp.mss ? kcp.mss : len;
    const seg = {
      conv: 0, cmd: 0, frg: (kcp.stream === 0) ? (count - i - 1) : 0,
      wnd: 0, ts: 0, sn: 0, una: 0, len: size,
      resendts: 0, rto: 0, fastack: 0, xmit: 0,
      data: Buffer.from(buffer.subarray(0, size)),
    };
    kcp.snd_queue.push(seg);
    kcp.nsnd_que = (kcp.nsnd_que + 1) >>> 0;
    buffer = buffer.subarray(size);
    len -= size;
  }
  return 0;
}

function setmtu(kcp, mtu) {
  if (mtu < 50 || mtu < IKCP_OVERHEAD) return -1;
  kcp.mtu = mtu;
  kcp.mss = kcp.mtu - IKCP_OVERHEAD;
  kcp.buffer = Buffer.alloc((kcp.mtu + IKCP_OVERHEAD) * 3);
  return 0;
}

function interval(kcp, iv) {
  if (iv > 5000) iv = 5000;
  else if (iv < 10) iv = 10;
  kcp.interval = iv;
  return 0;
}

function nodelay(kcp, nd, iv, resend, nc) {
  if (nd >= 0) {
    kcp.nodelay = nd;
    kcp.rx_minrto = nd ? IKCP_RTO_NDL : IKCP_RTO_MIN;
  }
  if (iv >= 0) {
    if (iv > 5000) iv = 5000; else if (iv < 10) iv = 10;
    kcp.interval = iv;
  }
  if (resend >= 0) kcp.fastresend = resend;
  if (nc >= 0) kcp.nocwnd = nc;
  return 0;
}

function wndsize(kcp, sndwnd, rcvwnd) {
  if (kcp) {
    if (sndwnd > 0) kcp.snd_wnd = sndwnd;
    if (rcvwnd > 0) kcp.rcv_wnd = imax(rcvwnd, IKCP_WND_RCV);
  }
  return 0;
}

function waitsnd(kcp) {
  return (kcp.nsnd_buf + kcp.nsnd_que) >>> 0;
}

function getconv(ptr) {
  return ptr.readUInt32LE(0) >>> 0;
}

module.exports = {
  create, release, setoutput, recv, send, update, check, input, flush,
  peeksize, setmtu, wndsize, waitsnd, nodelay, interval, getconv,
  CMD_PUSH: IKCP_CMD_PUSH, CMD_ACK: IKCP_CMD_ACK,
  CMD_WASK: IKCP_CMD_WASK, CMD_WINS: IKCP_CMD_WINS,
  OVERHEAD: IKCP_OVERHEAD,
};
