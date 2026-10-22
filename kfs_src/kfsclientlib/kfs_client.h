//=====================================================================
// kfs_client.h — 客户端库内部共享声明（不对外发布）
//
//   仅供 kfs_client_core.c（驱动线程 + 公共 C ABI 实现）使用。
//   宿主 / 绑定层只允许依赖 kfs_client_api.h，不得包含本头。
//   公共工具（kfs_now_ms）见 common/kfs_common.h。
//
//   分层（本次重构后）：
//     kfs_client_api.h   语言无关 C ABI（Unity/Go/Lua 等所有绑定的契约）
//     kfs_client_core.c  驱动线程 + 解帧/对账/事件队列 + ABI 实现（无 Lua）
//     kfs_client_lua.c   Lua 薄绑定：仅指针包装与值映射，零逻辑
//=====================================================================
#ifndef KFS_CLIENT_H
#define KFS_CLIENT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stddef.h>

#include <pthread.h>
#include <time.h>
#include <arpa/inet.h>          /* inet_pton */

#include "net.h"
#include "frame.h"
#include "kcp/ikcp.h"
#include "kfs_common.h"
#include "log.h"
#include "kfs_code.h"
#include "kfs_client_api.h"     /* 公共 ABI：kfs_client 前向声明 / 事件 / 状态码 */

//---------------------------------------------------------------------
// 内部事件队列
//---------------------------------------------------------------------
// 容量：广播帧积压上限。按 50fps 计约 10s；宿主卡顿/切后台超出时
// 丢最旧帧（追帧语义，旧帧对状态同步无价值），并保留最新。
#define KFS_C_EVENT_CAP 512

// 队列槽：data 为 malloc 变长负载副本；pop 时所有权移交取出方
// （由 kfs_client_event_release 归还），避免二次拷贝。
typedef struct kfs_cevent {
    int      type;        // kfs_client_event_type
    uint32_t fid;         // 帧号
    int      consistent;  // FRAME：广播负载末尾对账哈希是否与重算一致
    int      len;         // data 字节数（0 时 data 可为 NULL）
    char    *data;
} kfs_cevent;

//---------------------------------------------------------------------
// 客户端对象（单会话）。字段全部由 c->lock 保护（创建后单线程除外）。
//---------------------------------------------------------------------
struct kfs_client {
    kfs_net *net;               // 底层网络（net.c）
    ikcpcb  *kcp;               // 到服务器的 KCP 会话
    uint32_t conv;              // KCP conv（与服务器 add_player 一致）
    int      closed;            // 1=已关闭

    // JOIN 状态机（驱动线程写，ABI 读；in_join 支撑非阻塞 join_check）
    int      joined;            // 已收到服务器 JOIN 应答（无论成败）
    int      join_ok;           // JOIN 是否成功（1=已登记进房间）
    uint8_t  join_result;       // 服务器 result 码（kfs_code）
    int      in_join;           // 1=join_start 已发出、等待 ACK
    uint64_t join_begin_ms;     // join_start 时刻（超时判定）

    pthread_mutex_t lock;       // 保护 net/kcp/closed/状态机/事件队列
    pthread_t   dthread;        // 后台驱动线程
    int         drunning;       // 驱动线程运行标志
    int         djoined;        // 驱动线程已 join

    int         heartbeat_ms;   // 自动心跳间隔（0=不启用）；驱动线程按此补发 HEARTBEAT
    uint64_t    last_heartbeat_ms;  // 上次发心跳的时间（单调时钟 ms）

    // 会话锚点（驱动线程解析成功 JOIN ACK 时写入）
    uint32_t fi;                // 同步间隔 ms
    uint32_t seed;              // 房间随机因子（本地 RNG 用它回滚重算）
    int64_t  stime;             // 时钟差：客户端本地时刻 = 服务器时刻 + stime

    // 开战信号（驱动线程收到 ALL_READY 时写入）
    uint32_t start_fid;         // 服务器开战起始帧号
    int      started;           // 1=已收到 ALL_READY

    // 收件事件队列（驱动线程解帧 push；宿主经 poll_* 取走）
    kfs_cevent *queue;
    int         qcap, qhead, qtail, qcount;

    char errmsg[128];           // 最近一次失败描述（last_error 查询）
};

//---------------------------------------------------------------------
// 跨模块函数声明（kfs_client_core.c 实现）
//---------------------------------------------------------------------
extern int kfs_c_output(const char *buf, int len, ikcpcb *kcp, void *user);
extern void *kfs_drive_thread(void *arg);

#endif /* KFS_CLIENT_H */
