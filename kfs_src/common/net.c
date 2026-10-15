//=====================================================================
//
// net.c — KCP 帧同步库的网络连接层（纯 C，不依赖 Lua）
//
// 管理 UDP socket 生命周期：
//   net_open()   建 socket、绑定端口、启动接收线程
//   net_recv()   从有锁队列取一个原始 UDP 包
//   net_send()   向已学到的对端 sendto
//   net_set_peer() 显式指定对端（点对点）
//   net_close()  停线程、关 socket、释放
//
// 线程模型：
//   - 接收线程：仅 recvfrom 原始 UDP 包，放入有锁环形队列。
//   - 调用方（Lua 接入层）主线程：net_recv() 轮询取出，
//     并对首个包自动学习对端地址。
//   => 队列用互斥锁保护；对端地址也在锁内读写，线程安全。
//
//=====================================================================
#include "net.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include <pthread.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

//---------------------------------------------------------------------
// 接收队列元素
//---------------------------------------------------------------------
typedef struct {
    char data[KFS_NET_MAX_PKT];
    int  len;
    struct sockaddr_in from;
} net_pkt;

//---------------------------------------------------------------------
// 网络连接句柄
//---------------------------------------------------------------------
struct kfs_net {
    int sock;                       // UDP socket
    int running;                    // 接收线程运行标志
    pthread_t thread;               // 接收线程
    pthread_mutex_t qlock;          // 队列锁

    net_pkt *queue;                 // 环形队列
    int qcap, qhead, qtail, qcount;

    int peer_set;                   // 是否已学到对端
    struct sockaddr_in peer;        // 点对点对端地址

    char errmsg[128];               // 最近错误描述
};

// 进程级静态：net_open 失败时保留错误描述，供 net_errmsg(NULL) 查询
static char g_last_err[128];

// 写入错误信息
static void set_err(kfs_net *n, const char *fmt, int e)
{
    if (n) {
        if (e != 0)
            snprintf(n->errmsg, sizeof(n->errmsg), fmt, e);
        else
            snprintf(n->errmsg, sizeof(n->errmsg), "%s", fmt);
        snprintf(g_last_err, sizeof(g_last_err), "%s", n->errmsg);
    } else {
        if (e != 0)
            snprintf(g_last_err, sizeof(g_last_err), fmt, e);
        else
            snprintf(g_last_err, sizeof(g_last_err), "%s", fmt);
    }
}

//---------------------------------------------------------------------
// 内部：向队列压入一个包（接收线程调用）
//---------------------------------------------------------------------
static void queue_push(kfs_net *n, const char *data, int len,
                       const struct sockaddr_in *from)
{
    pthread_mutex_lock(&n->qlock);
    if (n->qcount < n->qcap) {
        net_pkt *slot = &n->queue[n->qtail];
        if (len > KFS_NET_MAX_PKT) len = KFS_NET_MAX_PKT;
        memcpy(slot->data, data, (size_t)len);
        slot->len = len;
        slot->from = *from;
        n->qtail = (n->qtail + 1) % n->qcap;
        n->qcount++;
    }
    // 队列满：丢弃（UDP 天然允许丢包）
    pthread_mutex_unlock(&n->qlock);
}

//---------------------------------------------------------------------
// 接收线程主函数
//---------------------------------------------------------------------
static void *recv_thread(void *arg)
{
    kfs_net *n = (kfs_net *)arg;
    char buf[KFS_NET_MAX_PKT];
    struct sockaddr_in from;
    socklen_t fromlen = sizeof(from);

    while (n->running) {
        int r = (int)recvfrom(n->sock, buf, sizeof(buf), 0,
                              (struct sockaddr *)&from, &fromlen);
        if (r < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                struct timespec ts = {0, 10 * 1000 * 1000}; // 10ms
                nanosleep(&ts, NULL);
                continue;
            }
            if (!n->running) break;   // socket 已关闭
            continue;
        }
        if (r > 0)
            queue_push(n, buf, r, &from);
    }
    return NULL;
}

//---------------------------------------------------------------------
// 接口实现
//---------------------------------------------------------------------
kfs_net *net_open(int port)
{
    kfs_net *n = (kfs_net *)calloc(1, sizeof(kfs_net));
    struct sockaddr_in addr;
    int flags, on;

    if (n == NULL)
        return NULL;
    n->sock = -1;
    n->running = 0;
    n->peer_set = 0;

    // UDP socket
    n->sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (n->sock < 0) {
        set_err(n, "socket create failed (errno %d)", errno);
        free(n);
        return NULL;
    }

    // 非阻塞 + 端口复用
    flags = fcntl(n->sock, F_GETFL, 0);
    fcntl(n->sock, F_SETFL, flags | O_NONBLOCK);
    on = 1;
    setsockopt(n->sock, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);
    if (bind(n->sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        set_err(n, "bind port %d failed (errno %d)", errno);
        close(n->sock);
        n->sock = -1;
        free(n);
        return NULL;
    }

    // 队列 + 锁
    n->queue = (net_pkt *)malloc(sizeof(net_pkt) * KFS_NET_QUEUE_CAP);
    if (n->queue == NULL) {
        set_err(n, "out of memory", 0);
        close(n->sock);
        n->sock = -1;
        free(n);
        return NULL;
    }
    pthread_mutex_init(&n->qlock, NULL);
    n->qcap = KFS_NET_QUEUE_CAP;
    n->qhead = n->qtail = n->qcount = 0;

    // 启动接收线程
    n->running = 1;
    if (pthread_create(&n->thread, NULL, recv_thread, (void *)n) != 0) {
        set_err(n, "thread create failed (errno %d)", errno);
        n->running = 0;
        free(n->queue);
        n->queue = NULL;
        close(n->sock);
        n->sock = -1;
        pthread_mutex_destroy(&n->qlock);
        free(n);
        return NULL;
    }

    n->errmsg[0] = '\0';
    return n;
}

int net_recv(kfs_net *n, char *out, int out_cap, struct sockaddr_in *from)
{
    int got = 0;
    if (n == NULL || out == NULL || out_cap <= 0)
        return 0;
    pthread_mutex_lock(&n->qlock);
    if (n->qcount > 0) {
        net_pkt *slot = &n->queue[n->qhead];
        got = slot->len < out_cap ? slot->len : out_cap;
        memcpy(out, slot->data, (size_t)got);
        if (from) *from = slot->from;
        n->qhead = (n->qhead + 1) % n->qcap;
        n->qcount--;
        // 自动学习对端（点对点）
        if (!n->peer_set && from) {
            n->peer = *from;
            n->peer_set = 1;
        }
    }
    pthread_mutex_unlock(&n->qlock);
    return got;   // >0: 实际字节数；0: 队列为空
}

int net_send(kfs_net *n, const char *buf, int len)
{
    int sent;
    if (n == NULL || buf == NULL || len <= 0)
        return -1;
    pthread_mutex_lock(&n->qlock);
    if (!n->peer_set) {
        pthread_mutex_unlock(&n->qlock);
        return -1;   // 尚未学到对端
    }
    sent = (int)sendto(n->sock, buf, (size_t)len, 0,
                       (struct sockaddr *)&n->peer, sizeof(n->peer));
    pthread_mutex_unlock(&n->qlock);
    return sent;
}

// 向指定对端发送（多人广播）。锁只保护队列/peer 读写一致，
// sendto 到不同对端天然允许并发，无需逐连接加锁。
int net_send_to(kfs_net *n, const struct sockaddr_in *peer,
                const char *buf, int len)
{
    int sent;
    if (n == NULL || peer == NULL || buf == NULL || len <= 0)
        return -1;
    pthread_mutex_lock(&n->qlock);
    sent = (int)sendto(n->sock, buf, (size_t)len, 0,
                       (const struct sockaddr *)peer, sizeof(*peer));
    pthread_mutex_unlock(&n->qlock);
    return sent;
}

int net_set_peer(kfs_net *n, const struct sockaddr_in *peer)
{
    if (n == NULL || peer == NULL)
        return 0;
    pthread_mutex_lock(&n->qlock);
    n->peer = *peer;
    n->peer_set = 1;
    pthread_mutex_unlock(&n->qlock);
    return 1;
}

int net_peer_known(kfs_net *n)
{
    int known = 0;
    if (n == NULL) return 0;
    pthread_mutex_lock(&n->qlock);
    known = n->peer_set;
    pthread_mutex_unlock(&n->qlock);
    return known;
}

void net_close(kfs_net *n)
{
    if (n == NULL)
        return;
    if (n->running) {
        n->running = 0;
        shutdown(n->sock, SHUT_RDWR);   // 打断 recvfrom
        pthread_join(n->thread, NULL);
        n->running = 0;
    }
    if (n->queue) {
        free(n->queue);
        n->queue = NULL;
    }
    if (n->sock >= 0) {
        close(n->sock);
        n->sock = -1;
    }
    pthread_mutex_destroy(&n->qlock);
    free(n);
}

const char *net_errmsg(kfs_net *n)
{
    if (n == NULL)
        return g_last_err[0] ? g_last_err : "net: ok";
    if (n->errmsg[0] == '\0')
        return "net: ok";
    return n->errmsg;
}
