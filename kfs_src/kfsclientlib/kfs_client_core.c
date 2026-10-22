//=====================================================================
// kfs_client_core.c — 客户端库底层（纯 C，不依赖 Lua）
//
// 本文件承载客户端全部驱动逻辑：
//   1. 驱动线程：收原始 UDP 包 → 识别握手 ACK（锚定帧间隔/种子/时钟差）
//      → 其余喂 KCP → ikcp_update 重传节拍 → 自动心跳
//      → 解帧路由：广播帧（含 FNV 对账）/ ALL_READY 入事件队列，其余丢弃。
//   2. 公共 C ABI（kfs_client_api.h）：生命周期、join（阻塞/异步两式）、
//      上行帧编码发送、事件取出（poll/catch_up）、参数查询与 KCP 调优。
//
// 绑定层（如 kfs_client_lua.c）只允许做指针包装与值映射，零业务逻辑；
// Unity/Go/Rust 等宿主按同一 ABI 接入。
//=====================================================================
#include "kfs_client.h"

#define KFS_JOIN_TIMEOUT_MS 5000    // join 等待 ACK 超时（与旧 Lua 行为一致）

static char g_create_err[128];      // create 失败时供 err_out 返回（进程级）

//---------------------------------------------------------------------
// KCP output 回调：可靠流出口即 UDP 发送
//---------------------------------------------------------------------
int kfs_c_output(const char *buf, int len, ikcpcb *kcp, void *user)
{
    kfs_client *c = (kfs_client *)user;
    (void)kcp;
    if (c == NULL || c->net == NULL)
        return -1;
    return net_send(c->net, buf, len);
}

//---------------------------------------------------------------------
// 内部：事件队列（调用方须持有 c->lock）
//---------------------------------------------------------------------
// 取一个可写槽：满则丢最旧帧事件（追帧语义：旧广播对状态同步无价值）。
static kfs_cevent *queue_slot_for_push(kfs_client *c)
{
    if (c->qcount >= c->qcap) {
        kfs_cevent *old = &c->queue[c->qhead];
        free(old->data);
        old->data = NULL;
        old->len = 0;
        c->qhead = (c->qhead + 1) % c->qcap;
        c->qcount--;
    }
    return &c->queue[c->qtail];
}

// 入队一个事件；data/len 拷贝为堆副本。失败（OOM）静默丢事件。
static void queue_push(kfs_client *c, int type, uint32_t fid,
                       int consistent, const char *data, int len)
{
    kfs_cevent *slot;
    char *copy = NULL;

    if (len > 0) {
        copy = (char *)malloc((size_t)len);
        if (copy == NULL)
            return;
        memcpy(copy, data, (size_t)len);
    }
    slot = queue_slot_for_push(c);
    slot->type = type;
    slot->fid = fid;
    slot->consistent = consistent;
    slot->len = len;
    slot->data = copy;
    c->qtail = (c->qtail + 1) % c->qcap;
    c->qcount++;
}

// 弹出一个事件（data 指针所有权移交取出方）。空返回 0。
static int queue_pop(kfs_client *c, kfs_client_event *ev)
{
    kfs_cevent *slot;
    if (c->qcount <= 0)
        return 0;
    slot = &c->queue[c->qhead];
    ev->type = slot->type;
    ev->fid = slot->fid;
    ev->consistent = slot->consistent;
    ev->len = slot->len;
    ev->data = (uint8_t *)slot->data;      // 所有权移交
    slot->data = NULL;
    slot->len = 0;
    c->qhead = (c->qhead + 1) % c->qcap;
    c->qcount--;
    return 1;
}

// 清空队列（free 所有未取负载）。
static void queue_clear(kfs_client *c)
{
    while (c->qcount > 0) {
        kfs_cevent *slot = &c->queue[c->qhead];
        free(slot->data);
        slot->data = NULL;
        slot->len = 0;
        c->qhead = (c->qhead + 1) % c->qcap;
        c->qcount--;
    }
}

//---------------------------------------------------------------------
// 内部：解一包握手 ACK（调用方须持有 c->lock）
//   [magic4][result1][frame_interval_ms4][room_seed4][server_time_ms8]=21B
//---------------------------------------------------------------------
static void handle_join_ack(kfs_client *c, const char *buf, int rcv)
{
    c->joined = 1;
    c->in_join = 0;
    c->join_result = (uint8_t)buf[4];
    c->join_ok = (c->join_result == KFS_CODE_SUCCESS);
    if (rcv >= 21) {
        uint32_t fi, seed;
        uint64_t stime;
        memcpy(&fi, buf + 5, 4);
        memcpy(&seed, buf + 9, 4);
        memcpy(&stime, buf + 13, 8);
        c->fi = fi;
        c->seed = seed;                 // 本地 RNG 用它回滚重算
        // 记录时钟差：客户端本地时刻 = 服务器时刻 + stime
        c->stime = (int64_t)(kfs_now_ms() - stime);
        LOG_DEBUGF("JOIN_ACK ok=%d result=%u fi=%u seed=%u stime=%llu",
            c->join_ok, (unsigned)c->join_result, fi, seed,
            (unsigned long long)stime);
    }
}

//---------------------------------------------------------------------
// 内部：从 KCP 接收缓冲解帧并路由（调用方须持有 c->lock）
//   BROADCAST → 对账后入队；ALL_READY → 记开战锚点并入队；其余丢弃。
//---------------------------------------------------------------------
static void kfs_c_drain_kcp(kfs_client *c, char *scratch)
{
    int size;

    while ((size = ikcp_peeksize(c->kcp)) >= KFS_FRAME2_HEADER_SIZE) {
        int r;
        uint8_t type;
        uint32_t fid;
        const char *data;
        int dlen;

        if (size > KFS_FRAME_MAX_TOTAL) {
            // 异常超长"坏帧"：buffer=NULL 且 len>=peeksize 时
            // ikcp_recv 不拷贝但会消费消息，安全摘除毒丸。
            ikcp_recv(c->kcp, NULL, size);
            LOG_DEBUGF("drop oversized kcp message %d", size);
            continue;
        }
        r = ikcp_recv(c->kcp, scratch, size);
        if (r < KFS_FRAME2_HEADER_SIZE)
            break;
        if (frame_decode2(scratch, r, &type, &fid, &data, &dlen) != 0)
            continue;                       // 帧头与长度不符：丢弃

        if (type == KFS_CMD_BROADCAST) {
            // 帧对账：负载末尾 4B 为服务器全员输入 FNV-1a32，重算比对。
            int consistent = 0;
            if (dlen >= 4) {
                uint32_t h_sent, h_calc;
                memcpy(&h_sent, data + dlen - 4, 4);
                h_calc = frame_fnv1a(data, dlen - 4);
                consistent = (h_calc == h_sent);
            }
            queue_push(c, KFS_EVENT_FRAME, fid, consistent, data, dlen);
        } else if (type == KFS_CMD_ALL_READY) {
            c->start_fid = fid;
            c->started = 1;
            queue_push(c, KFS_EVENT_ROOM_START, fid, 0, NULL, 0);
        }
        // 其他控制帧（HEARTBEAT/READY_ACK/未知）：库内消费，丢弃不入队。
    }
}

//---------------------------------------------------------------------
// 驱动线程：收包 → 握手识别 → 喂 KCP → update → 心跳 → 解帧入队
//---------------------------------------------------------------------
void *kfs_drive_thread(void *arg)
{
    kfs_client *c = (kfs_client *)arg;
    char buf[KFS_NET_MAX_PKT];
    char scratch[KFS_FRAME_MAX_TOTAL];
    struct sockaddr_in from;
    struct timespec ts = {0, 1 * 1000 * 1000};   // 1ms 节流
    IUINT32 now;
    int rcv;

    while (1) {
        pthread_mutex_lock(&c->lock);
        if (c->closed || c->net == NULL) {
            pthread_mutex_unlock(&c->lock);
            break;
        }
        now = kfs_now_ms();
        while ((rcv = net_recv(c->net, buf, KFS_NET_MAX_PKT, &from)) > 0) {
            uint32_t m;
            if (rcv >= 5) {
                memcpy(&m, buf, 4);
                if (m == KFS_HANDSHAKE_MAGIC) {
                    handle_join_ack(c, buf, rcv);
                    continue;                 // 握手包已消费，不喂 KCP
                }
            }
            if (c->kcp)
                ikcp_input(c->kcp, buf, (long)rcv);
        }
        if (c->kcp) {
            ikcp_update(c->kcp, now);
            kfs_c_drain_kcp(c, scratch);      // 解帧路由入事件队列
        }
        // 自动心跳：超过 heartbeat_ms 未发则补发（走 KCP 可靠传输，
        // 供服务器基于 last_active 做掉线判定）。心跳为空负载 HEARTBEAT 帧。
        if (c->kcp && c->heartbeat_ms > 0 &&
            (uint64_t)now - c->last_heartbeat_ms > (uint64_t)c->heartbeat_ms) {
            char hb[KFS_FRAME_MAX_TOTAL];
            int th = frame_encode2(hb, (int)sizeof(hb), KFS_CMD_HEARTBEAT, 0, NULL, 0);
            if (th > 0) {
                ikcp_send(c->kcp, hb, th);
                c->last_heartbeat_ms = now;
            }
        }
        pthread_mutex_unlock(&c->lock);
        nanosleep(&ts, NULL);
    }
    return NULL;
}

//---------------------------------------------------------------------
// 内部：上行帧公共发送路径（校验状态 → 编码 → ikcp_send）
//---------------------------------------------------------------------
static int kfs_send_frame(kfs_client *c, uint8_t type, uint32_t fid,
                          const void *data, int len)
{
    char pkt[KFS_FRAME_MAX_TOTAL];
    int total, ret;

    if (c == NULL)
        return KFS_ERR;
    if (len < 0 || len > KFS_FRAME_MAX_DATA)
        return KFS_ERR_PARAM;
    total = frame_encode2(pkt, (int)sizeof(pkt), type, fid,
                          (const char *)data, len);
    if (total < 0)
        return KFS_ERR_PARAM;

    pthread_mutex_lock(&c->lock);
    if (c->kcp == NULL || c->closed) {
        pthread_mutex_unlock(&c->lock);
        return KFS_ERR_CLOSED;
    }
    if (!c->joined || !c->join_ok) {
        pthread_mutex_unlock(&c->lock);
        return KFS_ERR_NOT_JOINED;
    }
    ret = ikcp_send(c->kcp, pkt, total);
    pthread_mutex_unlock(&c->lock);
    return (ret >= 0) ? KFS_OK : KFS_ERR;
}

static void kfs_set_err(kfs_client *c, const char *msg)
{
    if (c) {
        snprintf(c->errmsg, sizeof(c->errmsg), "%s", msg);
    } else {
        snprintf(g_create_err, sizeof(g_create_err), "%s", msg);
    }
}

//=====================================================================
// 公共 C ABI 实现
//=====================================================================

// 统一失败出口：把描述快照到进程级静态后整体释放。
// err_out 只允许指向 g_create_err —— c 释放后其 errmsg 不可读。
static kfs_client *kfs_create_fail(kfs_client *c, const char *msg,
                                   const char **err_out)
{
    snprintf(g_create_err, sizeof(g_create_err), "%s", msg);
    if (c) {
        if (c->kcp)
            ikcp_release(c->kcp);
        if (c->net)
            net_close(c->net);
        free(c->queue);
        pthread_mutex_destroy(&c->lock);
        free(c);
    }
    if (err_out) *err_out = g_create_err;
    return NULL;
}

kfs_client *kfs_client_create(uint16_t local_port, uint32_t conv,
                              const char **err_out)
{
    kfs_client *c = (kfs_client *)calloc(1, sizeof(kfs_client));

    if (c == NULL)
        return kfs_create_fail(NULL, "out of memory", err_out);
    c->conv = conv;
    pthread_mutex_init(&c->lock, NULL);

    c->queue = (kfs_cevent *)calloc(KFS_C_EVENT_CAP, sizeof(kfs_cevent));
    if (c->queue == NULL)
        return kfs_create_fail(c, "out of memory", err_out);
    c->qcap = KFS_C_EVENT_CAP;

    c->net = net_open((int)local_port);
    if (c->net == NULL)
        return kfs_create_fail(c, net_errmsg(NULL), err_out);

    c->kcp = ikcp_create(c->conv, (void *)c);
    if (c->kcp == NULL)
        return kfs_create_fail(c, "ikcp_create failed", err_out);
    ikcp_setoutput(c->kcp, kfs_c_output);
    ikcp_nodelay(c->kcp, 1, 10, 2, 1);       // 默认低延迟模式

    // 启动驱动线程
    c->drunning = 1;
    if (pthread_create(&c->dthread, NULL, kfs_drive_thread, (void *)c) != 0) {
        c->drunning = 0;
        c->djoined = 1;                       // 线程未起，无需 join
        return kfs_create_fail(c, "thread create failed", err_out);
    }

    if (err_out) *err_out = NULL;
    return c;
}

void kfs_client_destroy(kfs_client *c)
{
    if (c == NULL)
        return;
    // 置 closed，驱动线程见 closed 后不再访问 net/kcp
    pthread_mutex_lock(&c->lock);
    c->closed = 1;
    pthread_mutex_unlock(&c->lock);

    if (!c->djoined) {
        pthread_join(c->dthread, NULL);
        c->djoined = 1;
    }
    if (c->kcp) {
        ikcp_release(c->kcp);
        c->kcp = NULL;
    }
    if (c->net) {
        net_close(c->net);
        c->net = NULL;
    }
    queue_clear(c);
    free(c->queue);
    pthread_mutex_destroy(&c->lock);
    free(c);
    // 注意：本函数释放句柄本身，禁止对同一句柄二次调用。
    // Lua 绑定通过 close() 置空指针防止 __gc 再次销毁。
}

const char *kfs_client_last_error(kfs_client *c)
{
    if (c == NULL)
        return g_create_err[0] ? g_create_err : "ok";
    return c->errmsg[0] ? c->errmsg : "ok";
}

const char *kfs_client_version(void)
{
    return "kfsclient/1.1 abi1";
}

int kfs_client_set_peer(kfs_client *c, const char *ip, uint16_t port)
{
    struct sockaddr_in peer;
    int ret;

    if (c == NULL || ip == NULL)
        return KFS_ERR_PARAM;
    memset(&peer, 0, sizeof(peer));
    peer.sin_family = AF_INET;
    peer.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &peer.sin_addr) != 1) {
        kfs_set_err(c, "invalid ip");
        return KFS_ERR_PARAM;
    }
    pthread_mutex_lock(&c->lock);
    ret = (c->net && !c->closed) ? net_set_peer(c->net, &peer) : 0;
    pthread_mutex_unlock(&c->lock);
    if (ret != 1)
        return KFS_ERR_CLOSED;
    return KFS_OK;
}

// 发送裸 UDP JOIN 请求；调用方须持有 c->lock 之外的状态自行管理。
static int kfs_join_send(kfs_client *c, uint32_t room_id, uint32_t token)
{
    char hs[KFS_HS_LEN2];
    uint32_t m = KFS_HANDSHAKE_MAGIC;
    uint32_t rid = room_id;
    uint32_t tok = token;
    int ret;

    memcpy(hs, &m, 4);
    memcpy(hs + 4, &rid, 4);
    memcpy(hs + 8, &c->conv, 4);
    memcpy(hs + 12, &tok, 4);

    pthread_mutex_lock(&c->lock);
    if (c->net == NULL || c->closed) {
        pthread_mutex_unlock(&c->lock);
        return KFS_ERR_CLOSED;
    }
    c->joined = 0;
    c->join_ok = 0;
    c->join_result = 0xFF;
    c->in_join = 1;
    c->join_begin_ms = kfs_now_ms();
    ret = net_send(c->net, hs, KFS_HS_LEN2);
    if (ret < 0)
        c->in_join = 0;                       // 未发出：撤回 pending，防悬挂
    pthread_mutex_unlock(&c->lock);
    return (ret < 0) ? KFS_ERR_NO_PEER : KFS_OK;
}

int kfs_client_join(kfs_client *c, uint32_t room_id, uint32_t token)
{
    uint64_t deadline;
    int status;

    if (c == NULL)
        return KFS_ERR;
    status = kfs_join_send(c, room_id, token);
    if (status == KFS_ERR_CLOSED)
        return KFS_ERR_CLOSED;
    if (status == KFS_ERR_NO_PEER) {
        kfs_set_err(c, "no server peer, call setpeer first");
        return status;
    }

    // 等待驱动线程置 joined（与旧 Lua 行为一致：5ms 轮询，5s 超时）
    deadline = kfs_now_ms() + KFS_JOIN_TIMEOUT_MS;
    while (kfs_now_ms() < deadline) {
        int joined, ok;
        unsigned result;
        pthread_mutex_lock(&c->lock);
        joined = c->joined;
        ok = c->join_ok;
        result = c->join_result;
        pthread_mutex_unlock(&c->lock);
        if (joined) {
            if (ok)
                return KFS_OK;
            kfs_set_err(c, "server rejected join");
            snprintf(c->errmsg + strlen("server rejected join"),
                     sizeof(c->errmsg) - strlen("server rejected join"),
                     " (result=%u)", result);
            return KFS_ERR_REJECTED;
        }
        {
            struct timespec ts = {0, 5 * 1000 * 1000};
            nanosleep(&ts, NULL);
        }
    }
    // 超时：撤掉 pending 标记，避免 join_check 误判
    pthread_mutex_lock(&c->lock);
    c->in_join = 0;
    pthread_mutex_unlock(&c->lock);
    kfs_set_err(c, "join timeout");
    return KFS_ERR_TIMEOUT;
}

int kfs_client_join_start(kfs_client *c, uint32_t room_id, uint32_t token)
{
    int status;

    if (c == NULL)
        return KFS_ERR;
    status = kfs_join_send(c, room_id, token);
    if (status == KFS_ERR_NO_PEER)
        kfs_set_err(c, "no server peer, call setpeer first");
    return status;
}

int kfs_client_join_check(kfs_client *c)
{
    int joined, ok, in_join, result;
    uint64_t begin;

    if (c == NULL)
        return KFS_ERR;
    pthread_mutex_lock(&c->lock);
    joined = c->joined;
    ok = c->join_ok;
    in_join = c->in_join;
    result = c->join_result;
    begin = c->join_begin_ms;
    if (in_join && kfs_now_ms() - begin >= KFS_JOIN_TIMEOUT_MS)
        c->in_join = in_join = 0;             // 异步路径超时收敛
    pthread_mutex_unlock(&c->lock);

    if (joined) {
        if (ok)
            return KFS_OK;
        kfs_set_err(c, "server rejected join");
        snprintf(c->errmsg + strlen("server rejected join"),
                 sizeof(c->errmsg) - strlen("server rejected join"),
                 " (result=%d)", result);
        return KFS_ERR_REJECTED;
    }
    if (in_join)
        return KFS_ERR_PENDING;
    kfs_set_err(c, "join not started");
    return KFS_ERR_PARAM;
}

int kfs_client_send_input(kfs_client *c, uint32_t frame_id,
                          const void *data, int len)
{
    return kfs_send_frame(c, KFS_CMD_INPUT, frame_id, data, len);
}

int kfs_client_send_ready(kfs_client *c)
{
    return kfs_send_frame(c, KFS_CMD_READY, 0, NULL, 0);
}

int kfs_client_report_state(kfs_client *c, uint32_t frame_id, uint32_t state_hash)
{
    uint32_t hh = state_hash;
    return kfs_send_frame(c, KFS_CMD_STATE, frame_id, &hh, 4);
}

int kfs_client_poll_event(kfs_client *c, kfs_client_event *ev)
{
    int got = 0;

    if (c == NULL || ev == NULL)
        return -1;
    memset(ev, 0, sizeof(*ev));
    pthread_mutex_lock(&c->lock);
    if (c->closed || c->kcp == NULL) {
        pthread_mutex_unlock(&c->lock);
        return -1;
    }
    got = queue_pop(c, ev);
    pthread_mutex_unlock(&c->lock);
    return got;
}

int kfs_client_poll_frame(kfs_client *c, kfs_client_event *ev)
{
    if (c == NULL || ev == NULL)
        return -1;

    while (1) {
        kfs_client_event tmp;
        int r = kfs_client_poll_event(c, &tmp);
        if (r != 1)
            return r;
        if (tmp.type == KFS_EVENT_FRAME) {
            *ev = tmp;
            return 1;
        }
        // 非帧事件：ROOM_START 已由驱动线程记账（start_fid/started），
        // 这里静默消费，行为对齐旧 Lua recv_broadcast 对 ALL_READY 的处理。
        free(tmp.data);
    }
}

int kfs_client_catch_up(kfs_client *c, kfs_client_event *ev)
{
    kfs_client_event last;
    int got = 0;

    if (c == NULL || ev == NULL)
        return -1;
    memset(&last, 0, sizeof(last));
    memset(ev, 0, sizeof(*ev));

    // 事件队列即权威积压视图（驱动线程 ≤1ms 已把 KCP 缓冲解入队列）。
    while (kfs_client_poll_frame(c, ev) == 1) {
        free(last.data);
        last = *ev;
        got = 1;
        memset(ev, 0, sizeof(*ev));
    }
    if (got)
        *ev = last;
    return got;
}

void kfs_client_event_release(kfs_client_event *ev)
{
    if (ev == NULL)
        return;
    free(ev->data);
    ev->data = NULL;
    ev->len = 0;
}

int kfs_client_pending_events(kfs_client *c)
{
    int n;
    if (c == NULL)
        return 0;
    pthread_mutex_lock(&c->lock);
    n = c->qcount;
    pthread_mutex_unlock(&c->lock);
    return n;
}

uint32_t kfs_client_frame_interval_ms(kfs_client *c)
{
    uint32_t v;
    if (c == NULL) return 0;
    pthread_mutex_lock(&c->lock);
    v = c->fi;
    pthread_mutex_unlock(&c->lock);
    return v;
}

uint32_t kfs_client_room_seed(kfs_client *c)
{
    uint32_t v;
    if (c == NULL) return 0;
    pthread_mutex_lock(&c->lock);
    v = c->seed;
    pthread_mutex_unlock(&c->lock);
    return v;
}

int64_t kfs_client_server_time_offset_ms(kfs_client *c)
{
    int64_t v;
    if (c == NULL) return 0;
    pthread_mutex_lock(&c->lock);
    v = c->stime;
    pthread_mutex_unlock(&c->lock);
    return v;
}

uint32_t kfs_client_start_fid(kfs_client *c)
{
    uint32_t v;
    if (c == NULL) return 0;
    pthread_mutex_lock(&c->lock);
    v = c->start_fid;
    pthread_mutex_unlock(&c->lock);
    return v;
}

int kfs_client_is_started(kfs_client *c)
{
    int v;
    if (c == NULL) return 0;
    pthread_mutex_lock(&c->lock);
    v = c->started;
    pthread_mutex_unlock(&c->lock);
    return v;
}

int kfs_client_set_heartbeat(kfs_client *c, int ms)
{
    if (c == NULL)
        return KFS_ERR;
    pthread_mutex_lock(&c->lock);
    c->heartbeat_ms = (ms < 0) ? 0 : ms;
    c->last_heartbeat_ms = kfs_now_ms();    // 从现在起计时
    pthread_mutex_unlock(&c->lock);
    return KFS_OK;
}

int kfs_client_nodelay(kfs_client *c, int nodelay, int interval,
                       int resend, int nc)
{
    int ret;
    if (c == NULL)
        return KFS_ERR;
    pthread_mutex_lock(&c->lock);
    ret = (c->kcp) ? ikcp_nodelay(c->kcp, nodelay, interval, resend, nc)
                   : KFS_ERR_CLOSED;
    pthread_mutex_unlock(&c->lock);
    return ret;
}

int kfs_client_wndsize(kfs_client *c, int sndwnd, int rcvwnd)
{
    int ret;
    if (c == NULL)
        return KFS_ERR;
    pthread_mutex_lock(&c->lock);
    ret = (c->kcp) ? ikcp_wndsize(c->kcp, sndwnd, rcvwnd) : KFS_ERR_CLOSED;
    pthread_mutex_unlock(&c->lock);
    return ret;
}

int kfs_client_setmtu(kfs_client *c, int mtu)
{
    int ret;
    if (c == NULL)
        return KFS_ERR;
    pthread_mutex_lock(&c->lock);
    ret = (c->kcp) ? ikcp_setmtu(c->kcp, mtu) : KFS_ERR_CLOSED;
    pthread_mutex_unlock(&c->lock);
    return ret;
}

int kfs_client_is_closed(kfs_client *c)
{
    int v;
    if (c == NULL) return 1;
    pthread_mutex_lock(&c->lock);
    v = c->closed;
    pthread_mutex_unlock(&c->lock);
    return v;
}
