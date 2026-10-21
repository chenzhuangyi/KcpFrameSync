//=====================================================================
// kfs_client_core.c — 客户端库底层逻辑（不依赖 Lua）
//   KCP output 回调 + 后台驱动线程（收包喂 KCP + 驱动 KCP + 自动心跳）
//   结构定义见 kfs_client.h；公共工具见 common/kfs_common.h。
//=====================================================================
#include "kfs_client.h"

int kfs_c_output(const char *buf, int len, ikcpcb *kcp, void *user)
{
    kfs_client *c = (kfs_client *)user;
    (void)kcp;
    if (c == NULL || c->net == NULL)
        return -1;
    return net_send(c->net, buf, len);
}

void *kfs_drive_thread(void *arg)
{
    kfs_client *c = (kfs_client *)arg;
    char buf[KFS_NET_MAX_PKT];
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
                    c->joined  = 1;                 // 裸握手应答
                    c->join_ok = (buf[4] == 0);
                    // 成功JOIN应答：[magic4][result1][frame_interval_ms4][room_seed4][server_time_ms8]=21B
                    if (rcv >= 21) {
                        uint32_t fi   = 0;
                        uint32_t seed = 0;
                        uint64_t stime = 0;
                        memcpy(&fi,    buf + 5,  4);   // frame_interval_ms
                        memcpy(&seed,  buf + 9,  4);   // room_seed（与服务器 room->room_seed 同宽4B）
                        memcpy(&stime, buf + 13, 8);   // server_time_ms

                        c->fi = fi;
                        c->seed = seed;    // 存下，本地 RNG 用它回滚重算
                        // 记录 客户端↔服务器 时钟差：客户端本地时刻 = server_time + offset
                        c->stime = (int64_t)(kfs_now_ms() - stime);

                        LOG_DEBUGF("JOIN_ACK ok=%d fi=%u seed=%u stime=%llu",
                            (int)c->join_ok, fi, seed, (unsigned long long)stime);
                    }
                    continue;                         // 握手包已消费，不喂 KCP
                }
            }
            if (c->kcp)
                ikcp_input(c->kcp, buf, (long)rcv);
        }
        if (c->kcp)
            ikcp_update(c->kcp, now);
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
