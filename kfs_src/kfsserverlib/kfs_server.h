//=====================================================================
// kfs_server.h — 服务器库内部共享声明
//
// 供 server/kfs_core.c（底层逻辑）与 server/kfs_lua.c（Lua 绑定）共用：
//   - 全部结构定义（netref / 房间 / 玩家 / Lua userdata 对象）
//   - 跨模块函数声明（core 实现，lua 绑定调用）
// 公共工具（kfs_now_ms / kfs_rng_next）见 common/kfs_common.h。
//=====================================================================
#ifndef KFS_SERVER_H
#define KFS_SERVER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>

#include <pthread.h>
#include <time.h>
#include <arpa/inet.h>          /* inet_pton */

#include "net.h"
#include "frame.h"
#include "kcp/ikcp.h"
#include "kfs_common.h"
#include "log.h"
#include "kfs_code.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

//---------------------------------------------------------------------
// 常量
//---------------------------------------------------------------------
// 房间最大玩家数
#define KFS_ROOM_MAX_PLAYER 32
// 单帧单玩家输入上限（原型定长，可改动态）
#define KFS_ROOM_INPUT_MAX  512

// 超前输入窗口：玩家 KCP 里可能同时缓存未来多帧输入，用环形缓冲暂存，
// 容量须 ≥ 输入提前量窗口 K（+ 余量），供服务器按帧号取用。
#define KFS_PLAYER_BUF 8

//---------------------------------------------------------------------
// 结构定义
//---------------------------------------------------------------------
// 一帧超前输入（环形缓冲槽）
typedef struct kfs_pframe {
    uint32_t fid;           // 帧号
    int      valid;         // 该槽是否已填充
    int      len;           // 输入长度
    char     data[KFS_ROOM_INPUT_MAX];
} kfs_pframe;

// 玩家会话 token 登记项（per-player 鉴权）
typedef struct kfs_ptoken {
    uint32_t conv;          // 玩家 KCP conv
    uint32_t token;         // 该玩家专属 token
    int      valid;         // 是否已登记
} kfs_ptoken;

typedef struct kfs_netref kfs_netref;

// KCP 发送上下文：kfs_output 回调的 user。
//   peer == NULL → net_send（点对点房间，发已学对端）
//   peer != NULL → net_send_to（多玩家房间，按该玩家对端广播）
typedef struct kfs_send_ctx {
    kfs_netref *r;
    const struct sockaddr_in *peer;
} kfs_send_ctx;

// 玩家状态（kfs_player.state）：成员列表 players[] 单一数组，
// 由 state 区分三类占位；ready / offline 为附属标志。
typedef enum {
    KFS_PSTATE_INIT         = 0,   // 初始登记：创建房间时预登记（未连接），等待客户端 JOIN
    KFS_PSTATE_ONLINE       = 1,   // 已 JOIN 在线（参与收集/广播；ready/offline 附属）
    KFS_PSTATE_LEFT         = 2,   // 已转房/离开：席位不保留，不参与收集与广播
    KFS_PSTATE_DISCONNECT   = 3,   // 已掉线：席位保留，不参与收集与广播（预留）
} kfs_pstate;

// 玩家"在线"（有连接，驱动 update/收包/广播）
#define KFS_PLAYER_ONLINE(pl)  ((pl)->state == KFS_PSTATE_ONLINE)
// 玩家"活跃"（在线且未掉线，参与本帧集齐判定；掉线玩家不阻塞集齐）
#define KFS_PLAYER_ACTIVE(pl)  ((pl)->state == KFS_PSTATE_ONLINE && !(pl)->offline)

// 多玩家房间内的一个玩家会话
typedef struct kfs_player {
    uint32_t conv;              // KCP conv（玩家会话标识）
    int      state;             // KFS_PSTATE_*：INIT=初始登记 / ONLINE=在线 / LEFT=已转房
    int      ready;             // 已上报就绪（游戏数据加载完毕，业务层据此开战）
    int      offline;           // 掉线标记：ONLINE 超时置 1，席位保留等待重连；重连清除
    struct sockaddr_in peer;    // 该玩家 UDP 对端地址
    ikcpcb  *kcp;               // 该玩家 KCP 会话（INIT/LEFT 为 NULL）
    kfs_send_ctx ctx;           // 发送上下文（peer = &player->peer）
    uint32_t fid_last;          // 最近上报帧号（超时判定）
    int      room_index;        // 所属房间在 rooms 数组的下标（-1 表示未挂）
    int      slot_index;        // 在房间 players 数组的下标
    kfs_pframe pend[KFS_PLAYER_BUF];   // 超前输入环形缓冲（按 fid 取槽）
    uint64_t last_active_ms;    // 最后活跃时间（心跳/输入刷新，掉线判定）
    uint32_t state_hash[KFS_PLAYER_BUF]; // 各帧上报的逻辑状态哈希（对账）
    int      state_valid[KFS_PLAYER_BUF];// 该槽 state_hash 是否有效
} kfs_player;

// 多玩家房间：收集全员输入后广播
typedef struct kfs_room {
    uint32_t room_id;
    int      max_player;        // 设定的最大玩家数（创建时配置；满员判定/初始人数校验用）
    int      init_count;        // 初始玩家数（创建房间时预登记为 INIT；掉线不移除，席位保留等重连）
    int      player_count;      // 成员总数（players 数组占用 = INIT + ONLINE + LEFT 占位）
    kfs_player players[KFS_ROOM_MAX_PLAYER];

    int      phase;             // 0=准备(PREPARE) 1=对局中(PLAYING)；PLAYING 才收集推帧
    uint32_t collecting_fid;    // 当前待收集帧号
    uint32_t recv_mask;         // 位掩码：哪些玩家已上报本帧
    char  input_buf[KFS_ROOM_MAX_PLAYER][KFS_ROOM_INPUT_MAX];
    int   input_len[KFS_ROOM_MAX_PLAYER];

    uint64_t collect_begin_ms;  // 当前帧开始收集的时间（超时兜底用）
    uint64_t timeout_ms;        // 收集超时阈值

    int      tick_done;         // 当前驱动 tick 是否已推进一帧（帧率节拍限制）

    uint64_t create_ms;         // 房间创建时间（TTL 用）
    uint64_t ttl_ms;            // 房间最长保留时间（0=永久）
    uint32_t room_token;        // 房间级 token（0=不校验；per-player 未登记该 conv 时回退用）
    kfs_ptoken ptokens[KFS_ROOM_MAX_PLAYER];  // per-player 会话 token 表
    int        ptoken_count;    // 已登记玩家 token 数
    uint64_t idle_timeout_ms;   // 玩家掉线判定阈值（0=不启用）
    uint32_t sync_fid;          // 最近一致点（所有玩家状态一致的最高帧）
    int      sync_valid;        // 是否已有一致点
    int      desync;            // 1=检测到不同步（状态哈希不一致）
    uint32_t room_seed;         // 随机因子
} kfs_room;

// 点对点房间路由项（旧 create_room 兼容保留）
typedef struct kfs_route {
    IUINT32 conv;       // KCP 会话号（房间标识）
    ikcpcb *kcp;        // 该会话的 KCP 对象
} kfs_route;

// 一帧收件事件（驱动线程解出，Lua 经 conn:poll() 取走）
typedef struct kfs_event {
    uint32_t conv;      // 所属房间的 conv
    uint32_t fid;       // 帧号
    int      len;       // 负载长度
    char    *data;      // 负载（malloc；poll 取走后由 poll 释放）
} kfs_event;

// 网络句柄：连接与房间共享（引用计数 + closed + 自动驱动线程）
typedef struct kfs_netref {
    kfs_net *net;        // 底层网络连接（net.c）；closed 后为 NULL
    int      refs;       // 引用计数（连接 1 + 各房间）
    int      closed;     // 端口是否已关闭

    kfs_route *routes;  // 房间路由表（conv -> kcp）
    int         rcount; // 已注册房间数
    int         rcap;   // 路由表容量

    kfs_room *rooms;    // 多玩家房间表（服务器收集转发）
    int        room_count;
    int        room_cap;

    kfs_event *events;  // 收帧事件队列（驱动线程 push，poll 取走）
    int         ecount; // 未取事件数
    int         ecap;   // 事件容量

    uint32_t frame_interval_ms; // 驱动节拍：1000/frame_rate（帧同步节奏）

    pthread_mutex_t lock;   // 保护 net/closed/routes/各房间 kcp/事件队列
    pthread_t   dthread;    // 自动驱动线程
    int         drunning;   // 驱动线程运行标志
    int         djoined;    // 驱动线程已 join（防止重复 join）
} kfs_netref;

//---------------------------------------------------------------------
// Lua 对象定义（lua.c 使用）
//---------------------------------------------------------------------
// 连接实例（fs.new 返回）
typedef struct {
    kfs_netref *r;      // 网络句柄（独占持有）
} kfs_conn_t;

// 房间实例（conn:create_room 返回，点对点）
typedef struct {
    kfs_netref *r;      // 借用同一网络句柄（refs+1）
    ikcpcb     *kcp;    // KCP 会话
    kfs_send_ctx ctx;   // 发送上下文（点对点：peer=NULL，发已学对端）
} kfs_room_t;

// 服务器多玩家房间实例（conn:new_room 返回）
//   注意：rooms 数组会随 new_room realloc 移动，因此不存 raw 指针，
//   改存 room_id，每次用 kfs_room_find 定位（防悬挂指针）。
typedef struct {
    kfs_netref *r;
    uint32_t    room_id;    // 房间标识
    int         closed;     // 1=已关闭
} kfs_srvroom_t;

//---------------------------------------------------------------------
// 跨模块函数声明（kfs_core.c 实现；kfs_lua.c 调用）
//---------------------------------------------------------------------
// 网络句柄 / 路由
extern kfs_netref *netref_init(int port, uint32_t frame_interval_ms);
extern kfs_netref *netref_get(void);
extern kfs_netref *netref_addref(kfs_netref *r);
extern void netref_close(kfs_netref *r);
extern void netref_release(kfs_netref *r);
extern int  netref_route_add(kfs_netref *r, IUINT32 conv, ikcpcb *kcp);
extern void netref_route_del(kfs_netref *r, ikcpcb *kcp);
extern ikcpcb *netref_route_find(kfs_netref *r, IUINT32 conv);

// KCP output 回调（core 实现）
extern int kfs_output(const char *buf, int len, ikcpcb *kcp, void *user);

// 房间查询
extern kfs_room *kfs_room_find(kfs_netref *r, uint32_t room_id);

// 服务器房间内部 C 层（core 实现；调用方须持有 r->lock）
extern int  kfs_srv_new_room_c(kfs_netref *r, uint32_t room_id,
                               uint64_t timeout_ms, uint32_t token,
                               uint64_t ttl_ms, uint64_t idle_timeout_ms,
                               const uint32_t *init_convs, int init_count);
extern int  kfs_srv_add_player_c(kfs_netref *r, kfs_room *room, uint32_t conv, uint32_t token,
                                 const struct sockaddr_in *peer);
extern int  kfs_srv_remove_player_c(kfs_netref *r, kfs_room *room, uint32_t conv);
extern int  kfs_srv_set_player_token_c(kfs_room *room, uint32_t conv, uint32_t token);
extern void kfs_srv_close_room_c(kfs_netref *r, kfs_room *room);

// 驱动线程入口（netref_init 启动）
extern void *kfs_drive_thread(void *arg);

#endif /* KFS_SERVER_H */
