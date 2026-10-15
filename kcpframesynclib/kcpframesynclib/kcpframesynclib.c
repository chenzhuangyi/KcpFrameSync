//=====================================================================
//
// kcpframesynclib — KCP 帧同步库的 Lua 5.4 绑定（C 模块，房间模型）
//
// 职责（Lua 接入层）：
//   - fs.new(port, frame_rate) : 绑定 UDP 端口，返回"连接"实例
//                                （frame_rate 为驱动帧节拍，默认 20）
//   - conn:create_room(conv) : 创建帧同步房间，返回"房间"实例
//   - room:xxx()             : 房间实例控制房间（KCP 收发/调优/关闭）
//
// 底层职责已拆分：
//   - net.c（net.h）   : UDP socket/接收线程/原始包收发，不依赖 Lua/KCP
//   - frame.c(frame.h) : 帧信封编解码协议，纯 C，不依赖 Lua/KCP/网络
//   - 本层只通过 net_recv()/net_send() 与网络层、frame_*() 与帧层交互，
//     完全不触碰 socket / 线程 / 帧字节布局。
//
// 对象模型（两层实例，共享底层网络句柄）：
//   - 连接实例（fs.new 返回）：持有 kfs_netref（引用计数的网络句柄），
//     负责创建房间、设置对端、关闭端口。
//   - 房间实例（create_room 返回）：持有 KCP 会话 + 借用同一 netref，
//     负责 KCP 收发/调优/关闭房间。
//   - netref 用引用计数 + closed 标志管理：无论 conn:close() 还是
//     room:close() 谁先谁后，都不会产生悬挂指针。
//
// 自动驱动（本层内置后台驱动线程）：
//   - fs.new() 绑定端口成功后，自动启动一个驱动线程 kfs_drive_thread。
//   - 该线程循环：
//       * 先判定路由表里是否存在有效房间（rcount > 0）。
//       * 有房间：从网络队列按 KCP conv 路由喂包，并对全部房间
//         ikcp_update()（同时驱动 send 侧 flush / recv 侧解包）。
//       * 无房间：空转休眠，不调用任何 ikcp_*。
//   - 因此服务器/客户端都不再需要手工调用 room:update() 或
//     conn:dispatch() 来驱动 KCP —— 绑定端口即自动开始驱动。
//
// 并发与锁：
//   - 驱动线程与 Lua 主线程会同时访问 KCP 状态，故 netref 内置一把
//     互斥锁 lock，保护：net 指针 / closed / 路由表 / 各房间 KCP。
//   - 规则：所有房间方法、create_room、setpeer、close 都先拿 lock。
//   - 持锁期间禁止触发 Lua 分配或 luaL_error（GC / longjmp 会死锁），
//     因此每个方法"锁内做 C 操作、锁外返回 Lua 值"。
//   - kfs_output（KCP 回调）在持锁上下文中被调用，直接读 room->r->net，
//     不再加锁，避免自锁死锁。
//
// 用法（Lua）：
//   -- 服务器：一个端口承载多个房间（多 conv），绑定即自动驱动
//   local fs = require("kcpframesynclib")
//   local conn, err = fs.new(9000)            -- 绑定 udp 9000，启动驱动线程
//   if not conn then error(err) end
//   local room1 = conn:create_room(1)         -- 房间 conv=1
//   local room2 = conn:create_room(2)         -- 房间 conv=2
//   while true do
//       local fid1, d1 = room1:recv_frame()   -- 收（驱动线程已 update）
//       local fid2, d2 = room2:recv_frame()
//       if fid1 then room1:send_frame(fid1, "ack:" .. d1) end
//       if fid2 then room2:send_frame(fid2, "ack:" .. d2) end
//   end
//   room1:close(); room2:close()
//   conn:close()
//
//   -- 客户端：单房间，连接服务器对应 conv
//   local c, err = fs.new(11000)              -- 客户端独立端口，绑定即自动驱动
//   local room = c:create_room(1)             -- 连 conv=1 的房间
//   c:setpeer("127.0.0.1", 9000)
//   while true do
//       room:send_frame(0, "hello")
//       local fid, data = room:recv_frame()
//   end
//   room:close(); c:close()
//
// 依赖：Lua 5.4 头文件、net.c、kcp/ikcp.c、-lpthread
// 编译：见 Makefile；产物 kcpframesynclib.so
//
//=====================================================================
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>
#include <arpa/inet.h>          /* inet_pton */

#include <pthread.h>
#include <time.h>

#include "net.h"
#include "frame.h"
#include "kcp/ikcp.h"
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

//=====================================================================
// 网络句柄（引用计数 + closed 标志 + 自动驱动线程）
//   连接实例与房间实例共享同一个 kfs_netref。
//   net 的创建/释放由 netref 统一管理，关闭顺序无关紧要。
//   同一端口（netref）可承载多个房间（多 conv），由路由表分发。
//   lock 保护：net/closed/routes/各房间 kcp，供驱动线程与主线程互斥。
//=====================================================================
//=====================================================================
// 多玩家房间（服务器收集转发）
//=====================================================================
typedef struct kfs_netref kfs_netref;      // 前向声明

// 房间最大玩家数与单帧单玩家输入上限（原型定长，可改动态）
#define KFS_ROOM_MAX_PLAYER 8
#define KFS_ROOM_INPUT_MAX  512

// 超前输入窗口：玩家 KCP 里可能同时缓存未来多帧输入，用环形缓冲暂存，
// 容量须 ≥ 输入提前量窗口 K（+ 余量），供服务器按帧号取用。
#define KFS_PLAYER_BUF 8
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
    KFS_PSTATE_INIT   = 0,   // 初始登记：创建房间时预登记（未连接），等待客户端 JOIN
    KFS_PSTATE_ONLINE = 1,   // 已 JOIN 在线（参与收集/广播；ready/offline 附属）
    KFS_PSTATE_LEFT   = 2,   // 已转房/离开：席位保留，不参与收集与广播
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
} kfs_room;

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

// 当前单调时钟毫秒数
static IUINT32 kfs_now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (IUINT32)((uint64_t)ts.tv_sec * 1000u
                     + (uint64_t)ts.tv_nsec / 1000000u);
}

// 伪随机数生成（xorshift64*）——用于生成玩家会话 token。
//   非加密级（原型够用）；种子取单调时钟 + 自身地址，保证每次运行不同。
static uint64_t kfs_rng_state = 0;
static uint32_t kfs_rng_next(void)
{
    uint64_t x;
    if (kfs_rng_state == 0) {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        kfs_rng_state = (uint64_t)ts.tv_sec * 1000000000ull
                      + (uint64_t)ts.tv_nsec;
        kfs_rng_state ^= (uint64_t)(uintptr_t)&kfs_rng_state;
        if (kfs_rng_state == 0)
            kfs_rng_state = 0x9E3779B97F4A7C15ull;
    }
    x = kfs_rng_state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    kfs_rng_state = x;
    return (uint32_t)((x * 0x2545F4914F6CDD1Dull) >> 32);
}

// 自动驱动线程主函数（定义见后；netref_open 在此前向声明）
static void *kfs_drive_thread(void *arg);

// 创建句柄并绑定端口。失败返回 NULL（g_last_err 已设置）。
//   frame_interval_ms：驱动线程帧节拍（= 1000/frame_rate，最小 1ms）。
static kfs_netref *netref_open(int port, uint32_t frame_interval_ms)
{
    kfs_netref *r = (kfs_netref *)calloc(1, sizeof(kfs_netref));
    if (r == NULL)
        return NULL;
    r->net = net_open(port);
    if (r->net == NULL) {
        free(r);
        return NULL;
    }
    r->refs = 1;
    r->closed = 0;
    r->frame_interval_ms = (frame_interval_ms < 1) ? 1 : frame_interval_ms;
    r->routes = NULL;
    r->rcount = 0;
    r->rcap  = 0;
    pthread_mutex_init(&r->lock, NULL);
    r->drunning = 0;
    r->djoined = 1;             // 驱动线程尚未创建

    // 绑定端口成功后，自动启动驱动线程
    r->drunning = 1;
    if (pthread_create(&r->dthread, NULL, kfs_drive_thread, (void *)r) != 0) {
        r->drunning = 0;
        net_close(r->net);
        r->net = NULL;
        pthread_mutex_destroy(&r->lock);
        free(r);
        return NULL;
    }
    r->djoined = 0;
    return r;
}

static kfs_netref *netref_addref(kfs_netref *r)
{
    if (r) r->refs++;
    return r;
}

// 停止驱动线程并关闭端口（幂等）。
//   设 drunning=0 / closed=1，置空 net，join 驱动线程。
//   须在主线程（非驱动线程自身）调用。
static void netref_stop_drive(kfs_netref *r)
{
    kfs_net *n;
    if (r == NULL || r->djoined)
        return;
    pthread_mutex_lock(&r->lock);
    r->drunning = 0;
    r->closed = 1;
    n = r->net;
    r->net = NULL;
    pthread_mutex_unlock(&r->lock);
    if (n)
        net_close(n);
    pthread_join(r->dthread, NULL);
    r->djoined = 1;
}

// 关闭端口（幂等）：关 net + 停驱动线程；结构保留直到引用归零
static void netref_close(kfs_netref *r)
{
    if (r == NULL || r->closed)
        return;
    netref_stop_drive(r);
}

// 释放一个引用；归零时关闭端口、停驱动线程并释放结构
static void netref_release(kfs_netref *r)
{
    if (r == NULL)
        return;
    if (--r->refs <= 0) {
        netref_stop_drive(r);
        free(r->routes);
        r->routes = NULL;
        free(r->rooms);
        r->rooms = NULL;
        pthread_mutex_destroy(&r->lock);
        free(r);
    }
}

// 注册一个房间到路由表；成功返回 0，conv 冲突返回 -1
// 调用方须已持有 r->lock
static int netref_route_add(kfs_netref *r, IUINT32 conv, ikcpcb *kcp)
{
    int i;
    kfs_route *nr;
    int newcap;

    for (i = 0; i < r->rcount; i++)
        if (r->routes[i].conv == conv)
            return -1;              // 同一端口下 conv 重复

    if (r->rcount >= r->rcap) {
        newcap = (r->rcap == 0) ? 8 : r->rcap * 2;
        nr = (kfs_route *)realloc(r->routes,
                                  (size_t)newcap * sizeof(kfs_route));
        if (nr == NULL)
            return -1;
        r->routes = nr;
        r->rcap = newcap;
    }
    r->routes[r->rcount].conv = conv;
    r->routes[r->rcount].kcp  = kcp;
    r->rcount++;
    return 0;
}

// 从路由表移除一个房间（按 kcp 匹配）
// 调用方须已持有 r->lock
static void netref_route_del(kfs_netref *r, ikcpcb *kcp)
{
    int i, j;
    if (r == NULL || r->routes == NULL)
        return;
    for (i = 0; i < r->rcount; i++) {
        if (r->routes[i].kcp == kcp) {
            for (j = i; j < r->rcount - 1; j++)
                r->routes[j] = r->routes[j + 1];
            r->rcount--;
            return;
        }
    }
}

// 按 conv 查路由；未找到返回 NULL
// 调用方须已持有 r->lock
static ikcpcb *netref_route_find(kfs_netref *r, IUINT32 conv)
{
    int i;
    if (r == NULL)
        return NULL;
    for (i = 0; i < r->rcount; i++)
        if (r->routes[i].conv == conv)
            return r->routes[i].kcp;
    return NULL;
}

//---------------------------------------------------------------------
// 多玩家房间：收集转发辅助函数（调用方须已持有 r->lock）
//---------------------------------------------------------------------
// 按 conv 查找在线玩家会话（仅 ONLINE 有 KCP，用于包路由）；未找到返回 NULL
static kfs_player *kfs_find_player(kfs_netref *r, uint32_t conv)
{
    int i, j;
    if (r == NULL || r->rooms == NULL)
        return NULL;
    for (i = 0; i < r->room_count; i++)
        for (j = 0; j < (int)r->rooms[i].player_count; j++)
            if (KFS_PLAYER_ONLINE(&r->rooms[i].players[j]) &&
                r->rooms[i].players[j].conv == conv)
                return &r->rooms[i].players[j];
    return NULL;
}

// 按 room_id 查找房间；未找到返回 NULL
static kfs_room *kfs_room_find(kfs_netref *r, uint32_t room_id)
{
    int i;
    if (r == NULL)
        return NULL;
    for (i = 0; i < r->room_count; i++)
        if (r->rooms[i].room_id == room_id)
            return &r->rooms[i];
    return NULL;
}

// 组装广播帧：payload = 逐玩家 [conv 4B][len 2B][data...] + 末尾 4B FNV 对账哈希
// 返回总字节数；失败返回 -1
static int kfs_room_build_broadcast(kfs_room *room, char *out, int out_cap)
{
    char payload[KFS_ROOM_MAX_PLAYER * (4 + 2 + KFS_ROOM_INPUT_MAX) + 4];
    char *p = payload;
    uint32_t h;
    int   i, len;

    for (i = 0; i < (int)room->player_count; i++) {
        uint16_t l;
        if (!KFS_PLAYER_ONLINE(&room->players[i]))
            continue;           // INIT/LEFT 未参与对局：不入广播
        memcpy(p, &room->players[i].conv, 4); p += 4;
        l = (uint16_t)room->input_len[i];
        memcpy(p, &l, 2); p += 2;
        len = room->input_len[i];
        if (len > 0) {
            memcpy(p, room->input_buf[i], (size_t)len);
            p += len;
        }
    }
    len = (int)(p - payload);
    // 帧对账：对逐玩家序列计算 FNV-1a32 并追加 4B，客户端重算 body 比对，
    // 不一致即判定该帧不同步。
    h = frame_fnv1a(payload, len);
    memcpy(p, &h, 4);
    p += 4;
    len += 4;
    return frame_encode2(out, out_cap, KFS_CMD_BROADCAST,
                         room->collecting_fid, payload, len);
}

// 每驱动 tick 尝试推进一帧：所有玩家 pend 里都有 collecting_fid 的输入
// 即集齐广播并推进。受 tick_done 限制（每 tick 每房间最多推进一帧）。
static void kfs_room_try_advance(kfs_netref *r, kfs_room *room)
{
    char pkt[KFS_FRAME_MAX_TOTAL];
    uint32_t fid;
    int total, i;

    if (room->player_count == 0 || room->tick_done)
        return;
    fid = room->collecting_fid;
    // 检查全员（在线且未掉线）pend 是否都有本帧输入。
    // 掉线玩家（offline=1）不阻塞集齐：广播时用其 input_buf 最近一次输入填充。
    for (i = 0; i < (int)room->player_count; i++) {
        kfs_player *pl = &room->players[i];
        kfs_pframe *pf;
        if (!KFS_PLAYER_ACTIVE(pl))
            continue;
        pf = &pl->pend[fid % KFS_PLAYER_BUF];
        if (!pf->valid || pf->fid != fid)
            return;                             // 本帧尚缺该玩家输入，等下个 tick
    }
    // 齐了：把本帧输入写回 input_buf/input_len（供广播构建与超时填充）
    for (i = 0; i < (int)room->player_count; i++) {
        kfs_player *pl = &room->players[i];
        kfs_pframe *pf;
        if (!KFS_PLAYER_ACTIVE(pl))
            continue;
        pf = &pl->pend[fid % KFS_PLAYER_BUF];
        room->input_len[i] = pf->len;
        if (pf->len > 0)
            memcpy(room->input_buf[i], pf->data, (size_t)pf->len);
    }
    total = kfs_room_build_broadcast(room, pkt, (int)sizeof(pkt));
    if (total < 0)
        return;
    for (i = 0; i < (int)room->player_count; i++)
        if (KFS_PLAYER_ONLINE(&room->players[i]))
            ikcp_send(room->players[i].kcp, pkt, total);   // → net_send_to
    room->collecting_fid++;                     // 推进帧号
    room->collect_begin_ms = kfs_now_ms();
    room->tick_done = 1;                        // 本 tick 已推进一帧
}

// 状态对账：当某帧所有玩家都上报了 state_hash 时，比对推进一致点。
//   全一致 → sync_fid 推进到该帧；不一致 → 标记 desync（不同步告警）。
//   state_hash 由游戏逻辑计算，经客户端 report_state 上报；库里只做收集比对。
static void kfs_room_sync_check(kfs_netref *r, kfs_room *room)
{
    uint32_t fid, first;
    int i;

    if (room->player_count == 0)
        return;
    fid = room->collecting_fid;         // 刚推进到的帧（广播帧号为 fid-1）
    if (fid == 0)
        return;
    fid = fid - 1;                      // 检查刚广播的那一帧
    // 全员（在线）是否都已上报该帧状态哈希；掉线玩家不参与对账
    for (i = 0; i < (int)room->player_count; i++) {
        kfs_player *pl = &room->players[i];
        if (!KFS_PLAYER_ONLINE(pl))
            continue;
        if (!pl->state_valid[fid % KFS_PLAYER_BUF] ||
            pl->state_hash[fid % KFS_PLAYER_BUF] == 0)
            return;                     // 尚未全员上报，等下个 tick
    }
    // 取第一个在线玩家的哈希作基准；无在线玩家则跳过对账
    first = 0;
    for (i = 0; i < (int)room->player_count; i++) {
        if (KFS_PLAYER_ONLINE(&room->players[i])) {
            first = room->players[i].state_hash[fid % KFS_PLAYER_BUF];
            break;
        }
    }
    if (i >= (int)room->player_count)
        return;
    for (i = 0; i < (int)room->player_count; i++) {
        kfs_player *pl = &room->players[i];
        if (!KFS_PLAYER_ONLINE(pl))
            continue;
        if (pl->state_hash[fid % KFS_PLAYER_BUF] != first) {
            room->desync = 1;           // 不同步：逻辑状态哈希不一致
            return;
        }
    }
    room->desync = 0;
    room->sync_fid = fid;               // 一致点推进
    room->sync_valid = 1;
}

// 从某玩家 KCP 会话解出完整帧，写入其超前输入缓冲（服务器侧驱动线程内调用）
static void kfs_player_recv_pump(kfs_netref *r, kfs_player *pl, uint64_t now_ms)
{
    char tmp[KFS_FRAME_MAX_TOTAL];
    int n;
    kfs_room *room;

    if (pl->room_index < 0 || pl->room_index >= r->room_count)
        return;
    room = &r->rooms[pl->room_index];

    while ((n = (int)ikcp_recv(pl->kcp, tmp, (int)sizeof(tmp))) > 0) {
        uint8_t type;
        uint32_t fid;
        const char *data;
        int dlen;
        kfs_pframe *pf;
        if (frame_decode2(tmp, n, &type, &fid, &data, &dlen) != 0)
            continue;                           // 坏帧：丢弃

        if (type == KFS_CMD_HEARTBEAT) {
            pl->last_active_ms = now_ms;        // 保活：刷新活跃时间
            // printf("心跳 conv=%u, now=%u\n", pl->conv, (uint32_t)now_ms);
            continue;
        }
        if (type == KFS_CMD_READY) {
            pl->ready = 1;                      // 客户端加载完毕，业务层可据此开战
            pl->last_active_ms = now_ms;
            continue;
        }
        if (type == KFS_CMD_STATE) {
            // 状态对账：客户端上报某帧逻辑状态哈希（4B）
            if (dlen >= 4) {
                uint32_t h;
                memcpy(&h, data, 4);
                pl->state_hash[fid % KFS_PLAYER_BUF] = h;
                pl->state_valid[fid % KFS_PLAYER_BUF] = 1;
            }
            pl->last_active_ms = now_ms;
            continue;
        }
        if (type != KFS_CMD_INPUT)
            continue;                           // 其他控制帧忽略

        // 输入合法性校验（P0-4）
        if (dlen > KFS_ROOM_INPUT_MAX)
            continue;                           // 超长输入：拒绝
        if ((uint32_t)(fid - room->collecting_fid) > KFS_PLAYER_BUF)
            continue;                           // 超前超过缓冲窗口：拒绝（防灌帧/异常帧号）

        // 写入超前输入缓冲（环形，按 fid 取槽）。不做即时集齐——由每 tick
        // 的 kfs_room_try_advance 统一推进。
        pf = &pl->pend[fid % KFS_PLAYER_BUF];
        pf->fid = fid;
        pf->valid = 1;
        pf->len = dlen;
        if (dlen > 0 && data)
            memcpy(pf->data, data, (size_t)dlen);
        pl->fid_last = fid;
        pl->last_active_ms = now_ms;            // 任何输入都算活跃
    }
}

// 可靠传输下慢玩家兜底：超过 timeout 未集齐，用最近完整帧输入广播推进，
// 避免死等。注：线上标准做法是判定掉线/降级，这里暂保留旧输入填充作 fallback。
static void kfs_room_timeout_tick(kfs_netref *r, kfs_room *room,
                                  uint64_t now_ms)
{
    char pkt[KFS_FRAME_MAX_TOTAL];
    int total, i;

    if (room->player_count == 0 || room->tick_done)
        return;
    if (now_ms - room->collect_begin_ms < room->timeout_ms)
        return;

    // input_buf 保留最近一次集齐广播时的全员输入，直接以其推进
    total = kfs_room_build_broadcast(room, pkt, (int)sizeof(pkt));
    if (total < 0)
        return;
    for (i = 0; i < (int)room->player_count; i++)
        if (KFS_PLAYER_ONLINE(&room->players[i]))
            ikcp_send(room->players[i].kcp, pkt, total);

    room->collecting_fid++;                     // 推进帧号
    room->collect_begin_ms = now_ms;
    room->tick_done = 1;                        // 本 tick 已推进一帧
}

// add_player_c / remove_player_c / close_room_c 定义于文件末尾，此处前向声明
// （供 handle_join 与驱动线程使用）
static int kfs_srv_add_player_c(kfs_netref *r, kfs_room *room, uint32_t conv,
                                const struct sockaddr_in *peer);
static int kfs_srv_remove_player_c(kfs_netref *r, kfs_room *room, uint32_t conv);
static void kfs_srv_close_room_c(kfs_netref *r, kfs_room *room);

// 校验某玩家(conv)的 JOIN token：
//   1) 若房间登记了该 conv 的 per-player token → 必须匹配；
//   2) 否则若启用房间级 room_token → 校验匹配；
//   3) 两者都未配置 → 放行。
//   返回 1 通过；0 拒绝。
static int kfs_room_check_token(kfs_room *room, uint32_t conv, uint32_t token)
{
    printf("[server C] JOIN 检验 token conv=%u, token=%u, ptoken_count=%d\n", conv, token, (int)room->ptoken_count);
    int i;
    for (i = 0; i < (int)room->ptoken_count; i++) {
        printf("[server C] JOIN token 匹配 i=%d, iconv=%u, itoken=%u\n", i, room->ptokens[i].conv, room->ptokens[i].token);
        if (room->ptokens[i].conv == conv){
            printf("[server C] JOIN token 匹配 conv=%u, token=%u\n", conv, token);
            if (room->ptokens[i].valid)
                return room->ptokens[i].token == token;   // 该 conv 有专属 token
            else
                return 0;
        }
    }
    // if (room->room_token != 0)
    //     return room->room_token == token;             // 回退房间级 token
    return 0;                                          // 未配置鉴权 → 放行
}

// 登记一个玩家(conv)的会话 token（per-player 鉴权）。
//   已登记则覆盖；返回 0 成功；-1 表满。
static int kfs_srv_set_player_token_c(kfs_room *room, uint32_t conv,
                                      uint32_t token)
{
    int i;
    for (i = 0; i < (int)room->ptoken_count; i++)
        if (room->ptokens[i].conv == conv) {
            room->ptokens[i].token = token;
            room->ptokens[i].valid = 1;
            return 0;
        }
    if (room->ptoken_count > KFS_ROOM_MAX_PLAYER)
        return -1;
    room->ptokens[room->ptoken_count].conv = conv;
    room->ptokens[room->ptoken_count].token = token;
    room->ptokens[room->ptoken_count].valid = 1;
    room->ptoken_count++;
    return 0;
}

// 处理裸 UDP JOIN 请求：从源地址学 peer 并登记到目标房间，回 ACK。
//   JOIN 请求：[ magic 4B ][ room_id 4B ][ conv 4B ][ token 4B ]（16B；12B 兼容 token=0）
//   JOIN 应答：[ magic 4B ][ result 1B ]（0成功 1不存在 2满 3conv冲突 4其他 5token错误）
//   调用方须已持有 r->lock（驱动线程内）。
static void kfs_srv_handle_join(kfs_netref *r, uint32_t room_id,
                                uint32_t conv, uint32_t token,
                                const struct sockaddr_in *peer)
{
    printf("[server C] kfs_srv_handle_join room_id=%d, conv=%u, token=%u\n", room_id, conv, token);
    kfs_room *room = kfs_room_find(r, room_id);
    char ack[5];
    uint32_t m = KFS_HANDSHAKE_MAGIC;
    uint8_t result;
    int rc;

    if (room == NULL) {
        printf("[server C] kfs_srv_handle_join 房间不存在 room_id=%d, conv=%u, token=%u\n", room_id, conv, token);
        result = 1;                               // 房间不存在
    } else if (!kfs_room_check_token(room, conv, token)) {
        printf("[server C] kfs_srv_handle_join token 错误 room_id=%d, conv=%u, token=%u\n", room_id, conv, token);
        result = 5;                               // token 错误（鉴权失败）
    } else {
        rc = kfs_srv_add_player_c(r, room, conv, peer);
        result = (rc == 0) ? 0                    // 成功
               : (rc == -1) ? 2                   // 不在房间
               : (rc == -2) ? 3                   // conv 已在房间
               : 4;                               // 其他

        printf("[server C] kfs_srv_handle_join result=%d room_id=%d, conv=%u, token=%u\n", result, room_id, conv, token);
    }
    memcpy(ack, &m, 4);
    ack[4] = (char)result;
    net_send_to(r->net, peer, ack, 5);            // 裸 ACK 发回客户端
}

//---------------------------------------------------------------------
// 自动驱动线程
//   绑定端口成功后由 netref_open 启动。循环：
//     1. 判定路由表里是否存在有效房间（rcount > 0）。
//     2. 无房间：空转休眠（不调用任何 ikcp_*）。
//     3. 有房间：从网络队列按 conv 路由喂包，并对全部房间
//        ikcp_update()（驱动 send 侧 flush / recv 侧解包）。
//---------------------------------------------------------------------
static void *kfs_drive_thread(void *arg)
{
    kfs_netref *r = (kfs_netref *)arg;
    char buf[KFS_NET_MAX_PKT];
    struct sockaddr_in from;
    struct timespec ts;
    uint64_t interval, next_tick;
    IUINT32 now;
    int i, rcv;

    interval = (uint64_t)r->frame_interval_ms;
    if (interval < 1) interval = 1;
    next_tick = (uint64_t)kfs_now_ms();

    while (1) {
        now = kfs_now_ms();
        if ((uint64_t)now < next_tick) {
            // 未到本 tick 时点：补差休眠（最多睡到下一 tick，防空转）。
            // 严格调度用绝对 next_tick，避免 sleep 累积漂移。
            uint64_t wait = next_tick - (uint64_t)now;
            if (wait > interval) wait = interval;
            ts.tv_sec = wait / 1000;
            ts.tv_nsec = (long)(wait % 1000) * 1000000L;
            nanosleep(&ts, NULL);
            continue;
        }

        // 到本 tick 时点：处理一帧（收包 + 驱动 + 收集推进）
        pthread_mutex_lock(&r->lock);
        if (!r->drunning || r->closed) {
            pthread_mutex_unlock(&r->lock);
            break;                              // 线程退出
        }
        if (r->rcount > 0 || r->room_count > 0) {
            // 每个驱动 tick：各房间最多推进一帧（帧率节拍）
            for (i = 0; i < r->room_count; i++)
                r->rooms[i].tick_done = 0;
            now = kfs_now_ms();
            // 收包：先识别裸 UDP JOIN 握手（带/不带 token），其余按 KCP conv 路由
            while ((rcv = net_recv(r->net, buf, KFS_NET_MAX_PKT, &from)) > 0) {
                IUINT32 m, conv;
                kfs_player *pl;
                if (rcv < 4)
                    continue;                   // 过短，丢弃
                memcpy(&m, buf, 4);
                if (m == KFS_HANDSHAKE_MAGIC &&
                    (rcv == KFS_HS_LEN || rcv == KFS_HS_LEN2)) {
                    uint32_t room_id, jconv, jtok = 0;
                    memcpy(&room_id, buf + 4, 4);
                    memcpy(&jconv, buf + 8, 4);
                    if (rcv >= KFS_HS_LEN2)
                        memcpy(&jtok, buf + 12, 4);
                    kfs_srv_handle_join(r, room_id, jconv, jtok, &from);
                    continue;
                }
                memcpy(&conv, buf, 4);          // KCP 包头 4 字节即 conv（host order）
                pl = kfs_find_player(r, conv);
                if (pl) {
                    ikcp_input(pl->kcp, buf, (long)rcv);
                    kfs_player_recv_pump(r, pl, (uint64_t)now);   // 写缓冲/保活
                } else {
                    ikcpcb *kcp = netref_route_find(r, conv);
                    if (kcp)
                        ikcp_input(kcp, buf, (long)rcv);
                    // 未匹配的 conv：丢弃（不属于本端口任何房间）
                }
            }
            // update 点对点房间
            for (i = 0; i < r->rcount; i++)
                ikcp_update(r->routes[i].kcp, now);
            // update 多玩家房间 + 写缓冲 + 每 tick 尝试推进一帧 + 对账 + 超时兜底
            for (i = 0; i < r->room_count; i++) {
                kfs_room *room = &r->rooms[i];
                int j;
                for (j = 0; j < (int)room->player_count; j++) {
                    kfs_player *pl = &room->players[j];
                    if (!KFS_PLAYER_ONLINE(pl))
                        continue;               // INIT/LEFT 无 KCP 会话，跳过
                    ikcp_update(pl->kcp, now);
                    kfs_player_recv_pump(r, pl, (uint64_t)now);
                }
                // 仅对局中推进帧：准备阶段（phase=0）不收集不广播，
                // 玩家连接靠心跳保活、ready 计数判就绪，由业务层调 room:start()。
                if (room->phase == 1) {
                    kfs_room_try_advance(r, room);
                    kfs_room_sync_check(r, room);
                    kfs_room_timeout_tick(r, room, (uint64_t)now);
                }
            }
            // P0-3 掉线判定：超过 idle_timeout 未活跃的在线玩家标记掉线。
            //   不掉席位：初始/普通玩家掉线都保留成员（offline=1，广播用其
            //   最近输入填充），等待同 conv 重连恢复；业务层可随时调
            //   remove_player 主动清理。
            for (i = 0; i < r->room_count; i++) {
                kfs_room *room = &r->rooms[i];
                int j;
                if (room->idle_timeout_ms == 0)
                    continue;
                for (j = 0; j < (int)room->player_count; j++) {
                    kfs_player *pl = &room->players[j];
                    if (KFS_PLAYER_ONLINE(pl) && !pl->offline &&
                        (uint64_t)now - pl->last_active_ms > room->idle_timeout_ms) {
                        pl->offline = 1;        // 标记掉线：席位保留，等待重连
                        printf("[server C] 玩家 conv=%u 掉线（席位保留，可重连）\n", pl->conv);
                    }
                }
            }
            // 房间 TTL：超过最长保留时间自动销毁（冗余清理僵尸房间）
            for (i = 0; i < r->room_count; ) {
                kfs_room *room = &r->rooms[i];
                if (room->ttl_ms != 0 &&
                    (uint64_t)now - room->create_ms > room->ttl_ms) {
                    kfs_srv_close_room_c(r, room);
                    continue;           // 房间表收缩，i 不前进
                }
                i++;
            }
        }
        pthread_mutex_unlock(&r->lock);

        // 严格帧调度：推进到下一 tick；若处理耗时已超过帧间隔则追赶
        next_tick += interval;
        if (next_tick < (uint64_t)kfs_now_ms())
            next_tick = (uint64_t)kfs_now_ms();
    }
    return NULL;
}

//=====================================================================
// 对象定义
//=====================================================================
// 连接实例（fs.new 返回）
typedef struct {
    kfs_netref *r;      // 网络句柄（独占持有）
} kfs_conn_t;

// 房间实例（conn:create_room 返回）
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

//=====================================================================
// 内部小工具
//=====================================================================
static kfs_conn_t *check_conn(lua_State *L, int idx)
{
    return (kfs_conn_t *)luaL_checkudata(L, idx, "kcpframesynclib");
}

static kfs_room_t *check_room(lua_State *L, int idx)
{
    return (kfs_room_t *)luaL_checkudata(L, idx, "kcpframesynclib.room");
}

static kfs_srvroom_t *check_srvroom(lua_State *L, int idx)
{
    return (kfs_srvroom_t *)luaL_checkudata(L, idx, "kcpframesynclib.srvroom");
}

//=====================================================================
// 方法前向声明（metatable 在工厂函数中引用，须先声明后使用）
//=====================================================================
// 连接实例方法
static int kfs_conn_create_room(lua_State *L);
static int kfs_conn_new_room(lua_State *L);
static int kfs_conn_dispatch(lua_State *L);
static int kfs_conn_setpeer(lua_State *L);
static int kfs_conn_closed(lua_State *L);
static int kfs_conn_close(lua_State *L);
static int kfs_conn_gc(lua_State *L);
// 房间实例方法
static int kfs_room_update(lua_State *L);
static int kfs_room_check(lua_State *L);
static int kfs_room_flush(lua_State *L);
static int kfs_room_input(lua_State *L);
static int kfs_room_send_frame(lua_State *L);
static int kfs_room_recv_frame(lua_State *L);
static int kfs_room_peeksize(lua_State *L);
static int kfs_room_waitsnd(lua_State *L);
static int kfs_room_send(lua_State *L);
static int kfs_room_recv(lua_State *L);
static int kfs_room_nodelay(lua_State *L);
static int kfs_room_wndsize(lua_State *L);
static int kfs_room_setmtu(lua_State *L);
static int kfs_room_getconv(lua_State *L);
static int kfs_room_closed(lua_State *L);
static int kfs_room_close(lua_State *L);
static int kfs_room_gc(lua_State *L);
// 服务器多玩家房间实例方法
static int kfs_srv_add_player(lua_State *L);
static int kfs_srv_remove_player(lua_State *L);
static int kfs_srv_set_player_token(lua_State *L);
static int kfs_srv_get_id(lua_State *L);
static int kfs_srv_get_count(lua_State *L);
static int kfs_srv_start(lua_State *L);
static int kfs_srv_ready_count(lua_State *L);
static int kfs_srv_get_phase(lua_State *L);
static int kfs_srv_close(lua_State *L);
static int kfs_srv_gc(lua_State *L);

// KCP output 回调：KCP 产出的 UDP 数据交给网络层发送。
// 注意：本回调总是在持有 r->lock 的上下文中被调用
//（驱动线程 ikcp_update / 主线程 send/flush），故直接读 ctx->r->net，
// 不再加锁，避免自锁死锁。
//   ctx->peer == NULL → 点对点房间，发 net 已学对端（net_send）
//   ctx->peer != NULL → 多玩家房间，按该玩家对端广播（net_send_to）
static int kfs_output(const char *buf, int len, ikcpcb *kcp, void *user)
{
    kfs_send_ctx *ctx = (kfs_send_ctx *)user;
    kfs_net *n;
    (void)kcp;
    n = (ctx && ctx->r) ? ctx->r->net : NULL;
    if (n == NULL)
        return -1;
    if (ctx->peer)
        return net_send_to(n, ctx->peer, buf, len);
    return net_send(n, buf, len);
}

//=====================================================================
// 工厂：fs.new(port)  → 返回连接实例（只绑定端口，未创建房间）
//  绑定端口成功后，自动启动驱动线程（见 netref_open）。
//=====================================================================
static int kfs_new(lua_State *L)
{
    lua_Integer port = luaL_checkinteger(L, 1);
    lua_Integer fps  = luaL_optinteger(L, 2, 20);   // frame_rate，默认 20
    uint32_t interval;
    kfs_conn_t *self = (kfs_conn_t *)lua_newuserdatauv(L, sizeof(kfs_conn_t), 0);

    if (fps < 1) fps = 1;
    interval = (uint32_t)(1000 / fps);
    if (interval < 1) interval = 1;
    self->r = netref_open((int)port, interval);
    if (self->r == NULL) {
        lua_pushnil(L);
        lua_pushstring(L, net_errmsg(NULL));
        return 2;
    }

    if (luaL_newmetatable(L, "kcpframesynclib")) {
        static const luaL_Reg methods[] = {
            { "create_room", kfs_conn_create_room },
            { "new_room",    kfs_conn_new_room    },
            { "dispatch",    kfs_conn_dispatch    },
            { "setpeer",     kfs_conn_setpeer     },
            { "closed",      kfs_conn_closed      },
            { "close",       kfs_conn_close       },
            { "__gc",        kfs_conn_gc          },
            { NULL, NULL }
        };
        luaL_setfuncs(L, methods, 0);
        lua_pushliteral(L, "__index");
        lua_pushvalue(L, -2);
        lua_rawset(L, -3);
        lua_pushliteral(L, "__metatable");
        lua_pushliteral(L, "kcpframesynclib");
        lua_rawset(L, -3);
    }
    lua_setmetatable(L, -2);
    return 1;
}

//=====================================================================
// 连接实例方法
//=====================================================================
// conn:create_room(conv)  → 返回房间实例
// 注册路由表；同一端口下 conv 冲突则报错。
// 房间创建后，驱动线程的下一次循环即自动驱动它。
static int kfs_conn_create_room(lua_State *L)
{
    kfs_conn_t *conn = check_conn(L, 1);
    lua_Integer conv = luaL_optinteger(L, 2, 0);
    kfs_netref *r = conn->r;
    kfs_room_t *room;

    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: connection gone");

    // 先创建 Lua 对象与 KCP（锁外，避免持锁触发 GC）
    room = (kfs_room_t *)lua_newuserdatauv(L, sizeof(kfs_room_t), 0);
    room->r = netref_addref(r);
    room->kcp = NULL;
    room->ctx.r = r;
    room->ctx.peer = NULL;              // 点对点：发已学对端
    room->kcp = ikcp_create((IUINT32)conv, (void *)&room->ctx);
    if (room->kcp == NULL) {
        netref_release(room->r);
        room->r = NULL;
        return luaL_error(L, "kcpframesynclib: ikcp_create failed");
    }

    // 持锁注册路由 + 设置输出回调/默认参数
    pthread_mutex_lock(&r->lock);
    if (r->closed) {
        pthread_mutex_unlock(&r->lock);
        ikcp_release(room->kcp);
        room->kcp = NULL;
        netref_release(room->r);
        room->r = NULL;
        return luaL_error(L, "kcpframesynclib: connection closed");
    }
    if (netref_route_add(r, (IUINT32)conv, room->kcp) != 0) {
        pthread_mutex_unlock(&r->lock);
        ikcp_release(room->kcp);
        room->kcp = NULL;
        netref_release(room->r);
        room->r = NULL;
        return luaL_error(L, "kcpframesynclib: conv %d already in use",
                          (int)conv);
    }
    ikcp_setoutput(room->kcp, kfs_output);
    ikcp_nodelay(room->kcp, 1, 10, 2, 1);   // 默认快速模式，可被 Lua 覆盖
    pthread_mutex_unlock(&r->lock);

    // metatable（锁外）
    if (luaL_newmetatable(L, "kcpframesynclib.room")) {
        static const luaL_Reg methods[] = {
            { "update",      kfs_room_update      },
            { "check",       kfs_room_check       },
            { "flush",       kfs_room_flush       },
            { "input",       kfs_room_input       },
            { "send_frame",  kfs_room_send_frame  },
            { "recv_frame",  kfs_room_recv_frame  },
            { "peeksize",    kfs_room_peeksize    },
            { "waitsnd",     kfs_room_waitsnd     },
            { "send",        kfs_room_send        },
            { "recv",        kfs_room_recv        },
            { "nodelay",     kfs_room_nodelay     },
            { "wndsize",     kfs_room_wndsize     },
            { "setmtu",      kfs_room_setmtu      },
            { "getconv",     kfs_room_getconv     },
            { "closed",      kfs_room_closed      },
            { "close",       kfs_room_close       },
            { "__gc",        kfs_room_gc          },
            { NULL, NULL }
        };
        luaL_setfuncs(L, methods, 0);
        lua_pushliteral(L, "__index");
        lua_pushvalue(L, -2);
        lua_rawset(L, -3);
        lua_pushliteral(L, "__metatable");
        lua_pushliteral(L, "kcpframesynclib.room");
        lua_rawset(L, -3);
    }
    lua_setmetatable(L, -2);
    return 1;
}

// conn:dispatch(current_ms) — 手动驱动一次（兼容保留）
//   自动驱动线程已默认接管，通常无需调用；本方法仅供需要
//   手工触发单次收包+驱动的场景。驱动线程与它会互斥（共用 lock）。
static int kfs_conn_dispatch(lua_State *L)
{
    kfs_conn_t *conn = check_conn(L, 1);
    kfs_netref *r = conn->r;
    lua_Integer now = luaL_checkinteger(L, 2);
    char buf[KFS_NET_MAX_PKT];
    struct sockaddr_in from;
    int rcv, i;

    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: connection gone");
    pthread_mutex_lock(&r->lock);
    if (r->closed) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: connection closed");
    }
    while ((rcv = net_recv(r->net, buf, KFS_NET_MAX_PKT, &from)) > 0) {
        ikcpcb *kcp;
        IUINT32 conv;
        if (rcv < 4)
            continue;
        memcpy(&conv, buf, 4);
        kcp = netref_route_find(r, conv);
        if (kcp)
            ikcp_input(kcp, buf, (long)rcv);
    }
    for (i = 0; i < r->rcount; i++)
        ikcp_update(r->routes[i].kcp, (IUINT32)now);
    pthread_mutex_unlock(&r->lock);
    return 0;
}

// conn:setpeer(ip, port) — 显式指定对端地址（点对点）
static int kfs_conn_setpeer(lua_State *L)
{
    kfs_conn_t *conn = check_conn(L, 1);
    const char *ip = luaL_checkstring(L, 2);
    lua_Integer port = luaL_checkinteger(L, 3);
    struct sockaddr_in peer;
    kfs_netref *r = conn->r;

    memset(&peer, 0, sizeof(peer));
    peer.sin_family = AF_INET;
    peer.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &peer.sin_addr) != 1)
        return luaL_error(L, "kcpframesynclib: invalid ip '%s'", ip);

    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: connection gone");
    pthread_mutex_lock(&r->lock);
    if (r->closed || r->net == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: connection closed");
    }
    lua_pushinteger(L, net_set_peer(r->net, &peer));
    pthread_mutex_unlock(&r->lock);
    return 1;
}

// conn:closed() — 端口是否已关闭
static int kfs_conn_closed(lua_State *L)
{
    kfs_conn_t *conn = check_conn(L, 1);
    int closed = 1;
    if (conn->r) {
        pthread_mutex_lock(&conn->r->lock);
        closed = conn->r->closed;
        pthread_mutex_unlock(&conn->r->lock);
    }
    lua_pushboolean(L, closed);
    return 1;
}

// conn:close() — 关闭端口（停驱动线程；房间仍可存在，但其收发将失效）
static int kfs_conn_close(lua_State *L)
{
    kfs_conn_t *conn = check_conn(L, 1);
    if (conn->r)
        netref_close(conn->r);
    return 0;
}

// __gc：释放引用（不强制关闭端口；netref_release 归零时才释放）
static int kfs_conn_gc(lua_State *L)
{
    kfs_conn_t *conn = check_conn(L, 1);
    if (conn->r) {
        netref_release(conn->r);
        conn->r = NULL;
    }
    return 0;
}

//=====================================================================
// 房间实例方法（KCP 传输层，全部要求房间未关闭）
//   自动驱动线程已默认接管 update/收包，以下方法只需收发与调优。
//   统一持 r->lock，锁内不做 Lua 分配 / luaL_error。
//=====================================================================

// room:update(current_ms) — 仅驱动本房间（兼容；自动驱动已接管）
//   注意：自动驱动线程已在后台驱动全部房间，一般无需调用本方法。
//   为避免与驱动线程抢包，本方法不取队列（收包由驱动线程完成），
//   只对本房间执行 ikcp_update。
static int kfs_room_update(lua_State *L)
{
    kfs_room_t *room = check_room(L, 1);
    lua_Integer now = luaL_checkinteger(L, 2);
    kfs_netref *r = room->r;
    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: room closed");
    }
    ikcp_update(room->kcp, (IUINT32)now);
    pthread_mutex_unlock(&r->lock);
    return 0;
}

// room:check(current_ms) — 下次该 update 的时间
static int kfs_room_check(lua_State *L)
{
    kfs_room_t *room = check_room(L, 1);
    lua_Integer now = luaL_checkinteger(L, 2);
    kfs_netref *r = room->r;
    lua_Integer next;
    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: room closed");
    }
    next = (lua_Integer)ikcp_check(room->kcp, (IUINT32)now);
    pthread_mutex_unlock(&r->lock);
    lua_pushinteger(L, next);
    return 1;
}

// room:flush()
static int kfs_room_flush(lua_State *L)
{
    kfs_room_t *room = check_room(L, 1);
    kfs_netref *r = room->r;
    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: room closed");
    }
    ikcp_flush(room->kcp);
    pthread_mutex_unlock(&r->lock);
    return 0;
}

// room:input(data) — 手动喂包（若想绕过网络层/接收线程）
static int kfs_room_input(lua_State *L)
{
    kfs_room_t *room = check_room(L, 1);
    size_t len;
    const char *data = luaL_checklstring(L, 2, &len);
    kfs_netref *r = room->r;
    int ret;
    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: room closed");
    }
    ret = (int)ikcp_input(room->kcp, data, (long)len);
    pthread_mutex_unlock(&r->lock);
    lua_pushinteger(L, ret);
    return 1;
}

// room:peeksize()
static int kfs_room_peeksize(lua_State *L)
{
    kfs_room_t *room = check_room(L, 1);
    kfs_netref *r = room->r;
    int size;
    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: room closed");
    }
    size = ikcp_peeksize(room->kcp);
    pthread_mutex_unlock(&r->lock);
    lua_pushinteger(L, (lua_Integer)size);
    return 1;
}

// room:waitsnd()
static int kfs_room_waitsnd(lua_State *L)
{
    kfs_room_t *room = check_room(L, 1);
    kfs_netref *r = room->r;
    int n;
    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: room closed");
    }
    n = ikcp_waitsnd(room->kcp);
    pthread_mutex_unlock(&r->lock);
    lua_pushinteger(L, (lua_Integer)n);
    return 1;
}

// room:send(data) — 原始发送（不经帧封装）
static int kfs_room_send(lua_State *L)
{
    kfs_room_t *room = check_room(L, 1);
    size_t len;
    const char *data = luaL_checklstring(L, 2, &len);
    kfs_netref *r = room->r;
    int ret;
    if (len > (size_t)0x7FFF)
        return luaL_error(L, "kcpframesynclib: send data too large (%zu)", len);
    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: room closed");
    }
    ret = ikcp_send(room->kcp, data, (int)len);
    pthread_mutex_unlock(&r->lock);
    lua_pushinteger(L, ret);
    return 1;
}

// room:recv() — 原始接收，返回 data 或 nil
static int kfs_room_recv(lua_State *L)
{
    kfs_room_t *room = check_room(L, 1);
    kfs_netref *r = room->r;
    int size, ret;
    char *buf;
    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: room closed");
    }
    size = ikcp_peeksize(room->kcp);
    if (size < 0) {
        pthread_mutex_unlock(&r->lock);
        return 0;
    }
    buf = (char *)malloc((size_t)size);
    if (buf == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: out of memory");
    }
    ret = ikcp_recv(room->kcp, buf, size);
    pthread_mutex_unlock(&r->lock);
    if (ret < 0) { free(buf); return 0; }
    lua_pushlstring(L, buf, (size_t)ret);
    free(buf);
    return 1;
}

// room:nodelay(nodelay, interval, resend, nc)
static int kfs_room_nodelay(lua_State *L)
{
    kfs_room_t *room = check_room(L, 1);
    int nodelay = (int)luaL_checkinteger(L, 2);
    int interval = (int)luaL_checkinteger(L, 3);
    int resend   = (int)luaL_checkinteger(L, 4);
    int nc       = (int)luaL_checkinteger(L, 5);
    kfs_netref *r = room->r;
    int ret;
    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: room closed");
    }
    ret = ikcp_nodelay(room->kcp, nodelay, interval, resend, nc);
    pthread_mutex_unlock(&r->lock);
    lua_pushinteger(L, ret);
    return 1;
}

// room:wndsize(sndwnd, rcvwnd)
static int kfs_room_wndsize(lua_State *L)
{
    kfs_room_t *room = check_room(L, 1);
    int sndwnd = (int)luaL_checkinteger(L, 2);
    int rcvwnd = (int)luaL_checkinteger(L, 3);
    kfs_netref *r = room->r;
    int ret;
    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: room closed");
    }
    ret = ikcp_wndsize(room->kcp, sndwnd, rcvwnd);
    pthread_mutex_unlock(&r->lock);
    lua_pushinteger(L, ret);
    return 1;
}

// room:setmtu(mtu)
static int kfs_room_setmtu(lua_State *L)
{
    kfs_room_t *room = check_room(L, 1);
    int mtu = (int)luaL_checkinteger(L, 2);
    kfs_netref *r = room->r;
    int ret;
    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: room closed");
    }
    ret = ikcp_setmtu(room->kcp, mtu);
    pthread_mutex_unlock(&r->lock);
    lua_pushinteger(L, ret);
    return 1;
}

// room:getconv()
static int kfs_room_getconv(lua_State *L)
{
    kfs_room_t *room = check_room(L, 1);
    kfs_netref *r = room->r;
    lua_Integer conv;
    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: room closed");
    }
    conv = (lua_Integer)ikcp_getconv((const void *)room->kcp);
    pthread_mutex_unlock(&r->lock);
    lua_pushinteger(L, conv);
    return 1;
}

//=====================================================================
// 帧同步层绑定
//=====================================================================
// room:send_frame(frame_id, data) — 打包一帧并发送；返回 0 成功
static int kfs_room_send_frame(lua_State *L)
{
    kfs_room_t *room = check_room(L, 1);
    lua_Integer fid = luaL_checkinteger(L, 2);
    size_t len;
    const char *data = luaL_checklstring(L, 3, &len);
    char pkt[KFS_FRAME_MAX_TOTAL];
    int total;
    int ret;
    kfs_netref *r = room->r;

    if (len > (size_t)KFS_FRAME_MAX_DATA)
        return luaL_error(L, "kcpframesynclib: frame too large (%zu)", len);
    if (len > (size_t)0x7FFF - KFS_FRAME_HEADER_SIZE)
        return luaL_error(L, "kcpframesynclib: frame exceeds kcp message limit");
    total = frame_encode(pkt, (int)sizeof(pkt),
                         (uint32_t)(fid & 0xFFFFFFFFu), data, (int)len);
    if (total < 0)
        return luaL_error(L, "kcpframesynclib: frame encode failed");

    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: room closed");
    }
    ret = ikcp_send(room->kcp, pkt, total);
    pthread_mutex_unlock(&r->lock);
    lua_pushinteger(L, ret);
    return 1;
}

// room:recv_frame() — 解包一帧；返回 frame_id, data；无完整帧返回 nil
static int kfs_room_recv_frame(lua_State *L)
{
    kfs_room_t *room = check_room(L, 1);
    kfs_netref *r = room->r;
    int size, ret;
    char *pkt;
    uint32_t fid;
    const char *data;
    int dlen;

    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: room closed");
    }

    size = ikcp_peeksize(room->kcp);
    if (size < KFS_FRAME_HEADER_SIZE) {
        pthread_mutex_unlock(&r->lock);
        return 0;
    }
    if (size > KFS_FRAME_MAX_TOTAL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: bad frame size %d", size);
    }
    pkt = (char *)malloc((size_t)size);
    if (pkt == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "kcpframesynclib: out of memory");
    }
    ret = ikcp_recv(room->kcp, pkt, size);
    pthread_mutex_unlock(&r->lock);

    if (ret < KFS_FRAME_HEADER_SIZE) { free(pkt); return 0; }
    if (frame_decode(pkt, ret, &fid, &data, &dlen) != 0) {
        free(pkt);
        return luaL_error(L, "kcpframesynclib: bad frame payload");
    }
    lua_pushinteger(L, (lua_Integer)fid);
    lua_pushlstring(L, data, (size_t)dlen);
    free(pkt);
    return 2;
}

//=====================================================================
// 房间实例生命周期
//=====================================================================
// room:closed() — 房间是否已关闭（KCP 会话已释放）
static int kfs_room_closed(lua_State *L)
{
    kfs_room_t *room = check_room(L, 1);
    int closed = 1;
    if (room->r) {
        pthread_mutex_lock(&room->r->lock);
        closed = (room->kcp == NULL);
        pthread_mutex_unlock(&room->r->lock);
    }
    lua_pushboolean(L, closed);
    return 1;
}

// room:close() — 关闭房间（释放 KCP 会话，释放对网络句柄的借用）
//   先在锁内从路由表移除，再在锁外 ikcp_release（驱动线程不会再碰它）。
static int kfs_room_close(lua_State *L)
{
    kfs_room_t *room = check_room(L, 1);
    if (room->r) {
        pthread_mutex_lock(&room->r->lock);
        if (room->kcp) {
            netref_route_del(room->r, room->kcp);   // 从路由表移除
            ikcp_release(room->kcp);
            room->kcp = NULL;
        }
        pthread_mutex_unlock(&room->r->lock);
        netref_release(room->r);
        room->r = NULL;
    }
    return 0;
}

// __gc：兜底释放（与 room:close 幂等）
static int kfs_room_gc(lua_State *L)
{
    kfs_room_t *room = check_room(L, 1);
    if (room->r) {
        pthread_mutex_lock(&room->r->lock);
        if (room->kcp) {
            netref_route_del(room->r, room->kcp);   // 从路由表移除
            ikcp_release(room->kcp);
            room->kcp = NULL;
        }
        pthread_mutex_unlock(&room->r->lock);
        netref_release(room->r);
        room->r = NULL;
    }
    return 0;
}

//=====================================================================
// 服务器多玩家房间（收集转发）—— 内部 C 层
//   调用方须已持有 r->lock。
//=====================================================================
// 创建多玩家房间（调用方须已持有 r->lock）。
//   init_convs / init_count：初始玩家列表。创建时把初始玩家预登记为 INIT
//     占位（无连接），等客户端 JOIN 时升级为 ONLINE；初始玩家掉线不移除
//     （席位保留等待重连）。
//   校验：init_count 小于等于0 || 超过 KFS_ROOM_MAX_PLAYER 返回 -3；room_id 重复 -1；内存不足 -2。
static int kfs_srv_new_room_c(kfs_netref *r, uint32_t room_id,
                              uint64_t timeout_ms, uint32_t token,
                              uint64_t ttl_ms, uint64_t idle_timeout_ms,
                              const uint32_t *init_convs, int init_count)
{
    kfs_room *nr;
    int i;

    if (init_count <= 0 || init_count > KFS_ROOM_MAX_PLAYER)
        return -3;                              // 初始玩家数异常
    if (kfs_room_find(r, room_id) != NULL)
        return -1;                              // room_id 已存在
    if (r->room_count >= r->room_cap) {
        int newcap = (r->room_cap == 0) ? 4 : r->room_cap * 2;
        nr = (kfs_room *)realloc(r->rooms,
                                 (size_t)newcap * sizeof(kfs_room));
        if (nr == NULL)
            return -2;                          // 内存不足
        r->rooms = nr;
        r->room_cap = newcap;
    }
    nr = &r->rooms[r->room_count];
    memset(nr, 0, sizeof(*nr));
    nr->room_id = room_id;
    nr->max_player = 0;                // 设定的最大玩家数（满员判定用）
    nr->init_count = init_count;                // 初始玩家预登记数
    nr->player_count = 0;
    nr->phase = 0;                      // 准备阶段：等全员就绪后由业务层 start() 开推
    nr->collecting_fid = 0;
    nr->recv_mask = 0;
    nr->create_ms = kfs_now_ms();
    nr->timeout_ms = timeout_ms;
    nr->room_token = token;                     // 0=不校验
    nr->ttl_ms = ttl_ms;                        // 0=永久
    nr->idle_timeout_ms = idle_timeout_ms;      // 0=不启用掉线判定
    // 预登记初始玩家（INIT 占位：无连接、无 peer，等客户端 JOIN 升级 ONLINE）
    for (i = 0; i < init_count; i++) {
        kfs_player *pl = &nr->players[i];
        memset(pl, 0, sizeof(*pl));
        pl->conv = init_convs[i];
        pl->state = KFS_PSTATE_INIT;
        pl->room_index = (int)(nr - r->rooms);
        pl->slot_index = i;
        pl->last_active_ms = kfs_now_ms();      // 创建起即计活跃，避免立即被判掉线
    }
    nr->player_count = init_count;
    nr->ptoken_count = 0;
    r->room_count++;
    return 0;
}

// JOIN 登记：把玩家(conv)加入房间并建立连接（调用方须已持有 r->lock）。
//   1) 全局排重：该 conv 若已占位在**其他**房间（INIT/ONLINE）→ 先在旧房间
//      设置状态（置 LEFT 已转房、释放其 KCP、席位保留），再加入本房间——
//      防止同一玩家同时挂两个房间；
//   2) 目标房间内按 conv 复用槽位：
//        INIT（初始登记）      → 升级 ONLINE（首连）
//        ONLINE 且掉线(offline)→ 恢复 ONLINE（掉线重连）
//        ONLINE 且在线         → 返回 -2（重复 JOIN）
//        LEFT                  → 复活为 ONLINE（转房后回归）
//      无该 conv 槽位          → 末尾追加新成员 ONLINE（join 列表）
//   3) 建立/更新 KCP 会话与对端地址（掉线重连沿用已有 KCP，仅更新对端）。
//   返回 0 成功；-1 不在房间中；-2 conv 已在线在房；-3 KCP 创建失败。
static int kfs_srv_add_player_c(kfs_netref *r, kfs_room *room, uint32_t conv,
                                const struct sockaddr_in *peer)
{
    kfs_player *pl = NULL;
    int i, j;

    // 1) 全局排重：conv 已在其他房间（INIT/ONLINE）→ 旧房间设置状态 LEFT（转房）
    for (i = 0; i < r->room_count; i++) {
        kfs_room *or = &r->rooms[i];
        if (or == room)
            break;
        for (j = 0; j < (int)or->player_count; j++) {
            kfs_player *opl = &or->players[j];
            if (opl->conv == conv && opl->state != KFS_PSTATE_LEFT) {
                // 转房：旧房间设置状态（已离开），释放连接，席位保留
                if (opl->kcp) {
                    ikcp_release(opl->kcp);
                    opl->kcp = NULL;
                }
                opl->state = KFS_PSTATE_LEFT;
                opl->offline = 1;
                memset(opl->pend, 0, sizeof(opl->pend));
                break;
            }
        }
    }

    // 2) 目标房间内按 conv 找槽位
    for (i = 0; i < (int)room->player_count; i++) {
        if (room->players[i].conv == conv) {
            pl = &room->players[i];
            if (KFS_PLAYER_ACTIVE(pl))
                return -2;                      // 已在线在房：重复 JOIN
            break;                              // INIT / offline / LEFT：复用升级
        }
    }
    if (pl == NULL) {
        // 初始列表没有，就是有问题
        return -1;
    }

    // 3) 建立/更新连接：INIT/LEFT 建 KCP；掉线重连沿用已有 KCP，更新对端地址
    pl->peer = *peer;
    pl->ctx.r = r;
    pl->ctx.peer = &pl->peer;
    if (pl->kcp == NULL || pl->offline || pl->state == KFS_PSTATE_LEFT) {
        if (pl->kcp)
            ikcp_release(pl->kcp);              // 掉线重连/LEFT 回归：释放旧 KCP 重建，SN 对齐
        pl->kcp = ikcp_create(conv, (void *)&pl->ctx);
        if (pl->kcp == NULL)
            return -3;                          // KCP 创建失败
        ikcp_setoutput(pl->kcp, kfs_output);
        ikcp_nodelay(pl->kcp, 1, 10, 2, 1);
    }
    pl->state = KFS_PSTATE_ONLINE;             // 升级/恢复在线
    pl->offline = 0;
    pl->last_active_ms = kfs_now_ms();          // join 即视为活跃
    printf("[server C] kfs_srv_add_player_c 建立/更新连接 conv=%d, room_id=%u", conv, room->room_id);
    return 0;
}

// 主动移除玩家（业务层清理）：释放其 KCP（如有）、收缩成员数组。
//   任何占位（INIT/ONLINE/LEFT）都可被移除；返回 0 成功；-1 不在房间。
static int kfs_srv_remove_player_c(kfs_netref *r, kfs_room *room,
                                   uint32_t conv)
{
    int i, last;

    for (i = 0; i < (int)room->player_count; i++)
        if (room->players[i].conv == conv)
            break;
    if (i >= (int)room->player_count)
        return -1;                              // conv 不在房间
    if (room->players[i].kcp) {
        ikcp_release(room->players[i].kcp);
        room->players[i].kcp = NULL;
    }
    last = (int)room->player_count - 1;
    if (i != last) {
        room->players[i] = room->players[last];
        room->players[i].slot_index = i;
        room->players[i].ctx.peer = &room->players[i].peer;   // 修正指针
    }
    memset(&room->players[last], 0, sizeof(kfs_player));
    room->players[last].room_index = -1;
    room->players[last].slot_index = -1;
    room->player_count--;
    // 离房后本帧收集作废，等下一帧重新收集
    room->recv_mask = 0;
    room->collect_begin_ms = kfs_now_ms();
    memset(room->input_len, 0, sizeof(room->input_len));
    return 0;
}

static void kfs_srv_close_room_c(kfs_netref *r, kfs_room *room)
{
    int idx, i, j;

    idx = (int)(room - r->rooms);
    for (i = 0; i < (int)room->player_count; i++)
        if (room->players[i].kcp)
            ikcp_release(room->players[i].kcp);
    if (idx != r->room_count - 1)
        memmove(&r->rooms[idx], &r->rooms[idx + 1],
                (size_t)(r->room_count - idx - 1) * sizeof(kfs_room));
    r->room_count--;
    // 修正后续房间的 room_index
    for (i = idx; i < r->room_count; i++)
        for (j = 0; j < (int)r->rooms[i].player_count; j++)
            r->rooms[i].players[j].room_index = i;
}

//=====================================================================
// 服务器多玩家房间 —— Lua 绑定
//=====================================================================
// conn:new_room(room_id, max_player, timeout_ms, token, ttl_ms, idle_timeout_ms[, players])
//   参数：
//     room_id        房间标识
//     max_player     设定的最大玩家数（初始玩家数不得超过它；校验失败报错）
//     timeout_ms     本帧收集超时兜底阈值
//     token          房间级 token（0=不校验；per-player 未登记该 conv 时回退）
//     ttl_ms         房间最长保留时间（0=永久；超时自动销毁房间做冗余清理）
//     idle_timeout_ms 玩家掉线判定阈值（0=不启用；超时未活跃标记掉线，席位保留）
//     players        （可选）初始玩家 id 表（数组 {11,12} 或哈希 {[11]=true}，
//                     key 即玩家 conv）。创建房间时校验初始人数 ≤ max_player，
//                     把初始玩家预登记为 INIT（等客户端 JOIN 连接），并为每个
//                     玩家生成专属 token 返回 {[conv]=token}（per-player 鉴权）。
//                     初始玩家掉线不移除（席位保留等重连）。
//   返回：room 实例；传了 players 时返回 room, tokens 两个值。
static int kfs_conn_new_room(lua_State *L)
{
    kfs_conn_t *conn = check_conn(L, 1);
    lua_Integer rid  = luaL_checkinteger(L, 2);
    lua_Integer to   = luaL_optinteger(L, 3, 500);
    lua_Integer tok  = luaL_optinteger(L, 4, 0);      // token（0=不校验）
    lua_Integer ttl  = luaL_optinteger(L, 5, 0);      // 房间 TTL ms（0=永久）
    lua_Integer idle = luaL_optinteger(L, 6, 0);      // 掉线判定 ms（0=不启用）
    int has_players = (lua_type(L, 7) == LUA_TTABLE);
    uint32_t init_convs[KFS_ROOM_MAX_PLAYER];
    int init_count = 0;
    kfs_netref *r = conn->r;
    kfs_srvroom_t *srv;
    int rc, i;

    if (r == NULL)
        return luaL_error(L, "kcpframesynclib: connection gone");

    // 收集初始玩家列表（锁外遍历玩家表：key=玩家 id(conv)）
    if (has_players) {
        lua_pushnil(L);
        while (lua_next(L, 7) != 0) {
            // 设定的最大玩家数：1..KFS_ROOM_MAX_PLAYER-1（数组上限之外拒绝）
            if (init_count >= KFS_ROOM_MAX_PLAYER)
                return luaL_error(L, "kcpframesynclib: init_count error! init_count>%d", (int)KFS_ROOM_MAX_PLAYER);
        
            
            if (lua_type(L, -2) == LUA_TNUMBER &&
                init_count < KFS_ROOM_MAX_PLAYER)
                init_convs[init_count++] =
                    (uint32_t)(lua_tointeger(L, -2) & 0xFFFFFFFFu);
            lua_pop(L, 1);                  // pop value，key 留给下一轮 lua_next
        }
    }

    srv = (kfs_srvroom_t *)lua_newuserdatauv(L, sizeof(kfs_srvroom_t), 0);
    srv->r = netref_addref(r);
    srv->room_id = (uint32_t)(rid & 0xFFFFFFFFu);
    srv->closed = 0;

    // tokens 表（返回给 Lua）：[conv] = 服务器生成的玩家专属 token
    lua_newtable(L);

    // 底层创建房间：预登记初始玩家（INIT 占位，等客户端 JOIN 连接）
    pthread_mutex_lock(&r->lock);
    rc = kfs_srv_new_room_c(r, srv->room_id, (uint64_t)to,
                            (uint32_t)(tok & 0xFFFFFFFFu),
                            (uint64_t)ttl, (uint64_t)idle,
                            init_convs, init_count);
    pthread_mutex_unlock(&r->lock);
    if (rc != 0) {
        netref_release(srv->r);
        srv->r = NULL;
        return luaL_error(L, "kcpframesynclib: new_room failed (rc=%d)", rc);
    }

    // 诊断：打印初始玩家登记（临时调试，确认后删除）
    printf("[server C] new_room room=%u init_count=%d\n", (unsigned)srv->room_id, init_count);
    for (i = 0; i < init_count; i++)
        printf("[server C] 初始玩家 conv=%u\n", (unsigned)init_convs[i]);

    // 为每个初始玩家生成专属 token 并登记到房间实例（per-player 鉴权）
    for (i = 0; i < init_count; i++) {
        uint32_t t = kfs_rng_next();
        if (t == 0) t = kfs_rng_next();     // token 不为 0
        pthread_mutex_lock(&r->lock);
        {
            kfs_room *room = kfs_room_find(r, srv->room_id);
            if (room)
                kfs_srv_set_player_token_c(room, init_convs[i], t);
        }
        pthread_mutex_unlock(&r->lock);
        lua_pushinteger(L, (lua_Integer)init_convs[i]);  // 栈: [srv, tokens, conv]
        lua_pushinteger(L, (lua_Integer)t);             // [srv, tokens, conv, t]
        lua_rawset(L, -3);                              // tokens[conv]=t → [srv, tokens]
    }

    if (luaL_newmetatable(L, "kcpframesynclib.srvroom")) {
        static const luaL_Reg methods[] = {
            { "add_player",    kfs_srv_add_player    },
            { "remove_player", kfs_srv_remove_player },
            { "set_player_token", kfs_srv_set_player_token },
            { "get_id",        kfs_srv_get_id        },
            { "get_count",     kfs_srv_get_count     },
            { "start",         kfs_srv_start         },
            { "ready_count",   kfs_srv_ready_count   },
            { "get_phase",     kfs_srv_get_phase     },
            { "close",         kfs_srv_close         },
            { "__gc",          kfs_srv_gc            },
            { NULL, NULL }
        };
        luaL_setfuncs(L, methods, 0);
        lua_pushliteral(L, "__index");
        lua_pushvalue(L, -2);
        lua_rawset(L, -3);
        lua_pushliteral(L, "__metatable");
        lua_pushliteral(L, "kcpframesynclib.srvroom");
        lua_rawset(L, -3);
    }
    lua_setmetatable(L, -3);    // 栈: [srv(1), tokens(2), mt(3)] → srv 在 -3；setmetatable(srv) → [srv, tokens]
    return has_players ? 2 : 1;
}

// srvroom:add_player(conv, ip, port) — 手动登记一个玩家进房间
//   （也可直接靠客户端 JOIN 自动登记；含转房排重、INIT/掉线槽位复用）
//   返回 0 成功；-1 房间满；-2 conv 已在线在房；-3 KCP 创建失败；-4 房间已关闭
static int kfs_srv_add_player(lua_State *L)
{
    kfs_srvroom_t *srv = check_srvroom(L, 1);
    lua_Integer conv = luaL_checkinteger(L, 2);
    const char *ip  = luaL_checkstring(L, 3);
    lua_Integer port = luaL_checkinteger(L, 4);
    kfs_netref *r = srv->r;
    struct sockaddr_in peer;
    kfs_room *room;
    int rc;

    memset(&peer, 0, sizeof(peer));
    peer.sin_family = AF_INET;
    peer.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &peer.sin_addr) != 1)
        return luaL_error(L, "kcpframesynclib: invalid ip '%s'", ip);
    if (r == NULL || srv->closed)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    room = kfs_room_find(r, srv->room_id);
    rc = room ? kfs_srv_add_player_c(r, room, (uint32_t)(conv & 0xFFFFFFFFu),
                                     &peer) : -4;
    pthread_mutex_unlock(&r->lock);
    lua_pushinteger(L, rc);
    return 1;
}

// srvroom:remove_player(conv) — 移除玩家。返回 0 成功；-1 不在房间
static int kfs_srv_remove_player(lua_State *L)
{
    kfs_srvroom_t *srv = check_srvroom(L, 1);
    lua_Integer conv = luaL_checkinteger(L, 2);
    kfs_netref *r = srv->r;
    kfs_room *room;
    int rc;

    if (r == NULL || srv->closed)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    room = kfs_room_find(r, srv->room_id);
    rc = room ? kfs_srv_remove_player_c(r, room,
                                        (uint32_t)(conv & 0xFFFFFFFFu)) : -1;
    pthread_mutex_unlock(&r->lock);
    lua_pushinteger(L, rc);
    return 1;
}

// srvroom:set_player_token(conv, token) — 登记该玩家的专属会话 token
//   登记后该玩家 JOIN 必须携带相同 token（per-player 鉴权）。
//   返回 0 成功；-1 登记表满；-2 房间已不存在。
static int kfs_srv_set_player_token(lua_State *L)
{
    kfs_srvroom_t *srv = check_srvroom(L, 1);
    lua_Integer conv = luaL_checkinteger(L, 2);
    lua_Integer token = luaL_checkinteger(L, 3);
    kfs_netref *r = srv->r;
    kfs_room *room;
    int rc;

    if (r == NULL || srv->closed)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    room = kfs_room_find(r, srv->room_id);
    rc = room ? kfs_srv_set_player_token_c(room,
                    (uint32_t)(conv & 0xFFFFFFFFu),
                    (uint32_t)(token & 0xFFFFFFFFu)) : -2;
    pthread_mutex_unlock(&r->lock);
    lua_pushinteger(L, rc);
    return 1;
}

static int kfs_srv_get_id(lua_State *L)
{
    kfs_srvroom_t *srv = check_srvroom(L, 1);
    lua_pushinteger(L, (lua_Integer)srv->room_id);
    return 1;
}

// srvroom:get_count() — 房间成员总数（players 数组占用：
//   INIT 初始登记 + ONLINE 在线 + LEFT 已转房；掉线玩家席位保留仍计入）
static int kfs_srv_get_count(lua_State *L)
{
    kfs_srvroom_t *srv = check_srvroom(L, 1);
    kfs_netref *r = srv->r;
    int n = 0;

    if (r && !srv->closed) {
        pthread_mutex_lock(&r->lock);
        {
            kfs_room *room = kfs_room_find(r, srv->room_id);
            if (room) n = (int)room->player_count;
        }
        pthread_mutex_unlock(&r->lock);
    }
    lua_pushinteger(L, n);
    return 1;
}

// srvroom:start() — 业务层通知房间开始同步推帧（PREPARE → PLAYING）。
//   之后驱动线程每 tick 集齐全员输入即广播推进；此前客户端已发的超前
//   输入帧号仍有效（带 fid 写入 pend，不清空，避免与首批发帧竞态死锁）。
//   返回 0 成功；-1 房间不存在。
static int kfs_srv_start(lua_State *L)
{
    kfs_srvroom_t *srv = check_srvroom(L, 1);
    kfs_netref *r = srv->r;
    kfs_room *room;
    int rc = -1;

    if (r == NULL || srv->closed)
        return luaL_error(L, "kcpframesynclib: room gone");
    pthread_mutex_lock(&r->lock);
    room = kfs_room_find(r, srv->room_id);
    if (room) {
        room->phase = 1;                        // 进入对局：开始收集推帧
        room->collecting_fid = 0;
        room->recv_mask = 0;
        memset(room->input_len, 0, sizeof(room->input_len));
        room->collect_begin_ms = kfs_now_ms();
        room->tick_done = 0;
        rc = 0;
    }
    pthread_mutex_unlock(&r->lock);
    lua_pushinteger(L, rc);
    return 1;
}

// srvroom:ready_count() — 已上报就绪的玩家数（业务层据此判定全员就绪）
static int kfs_srv_ready_count(lua_State *L)
{
    kfs_srvroom_t *srv = check_srvroom(L, 1);
    kfs_netref *r = srv->r;
    int n = 0, j;

    if (r && !srv->closed) {
        pthread_mutex_lock(&r->lock);
        {
            kfs_room *room = kfs_room_find(r, srv->room_id);
            if (room)
                for (j = 0; j < (int)room->player_count; j++)
                    // 在线且未掉线且已就绪，才算"可开战就绪"
                    if (KFS_PLAYER_ACTIVE(&room->players[j]) && room->players[j].ready)
                        n++;
        }
        pthread_mutex_unlock(&r->lock);
    }
    lua_pushinteger(L, n);
    return 1;
}

// srvroom:get_phase() — 房间阶段：0=准备(PREPARE) 1=对局中(PLAYING)
static int kfs_srv_get_phase(lua_State *L)
{
    kfs_srvroom_t *srv = check_srvroom(L, 1);
    kfs_netref *r = srv->r;
    int ph = 0;

    if (r && !srv->closed) {
        pthread_mutex_lock(&r->lock);
        {
            kfs_room *room = kfs_room_find(r, srv->room_id);
            if (room) ph = room->phase;
        }
        pthread_mutex_unlock(&r->lock);
    }
    lua_pushinteger(L, ph);
    return 1;
}

// srvroom:close() — 关闭房间，释放全部玩家 KCP 并从房间表移除
static int kfs_srv_close(lua_State *L)
{
    kfs_srvroom_t *srv = check_srvroom(L, 1);

    if (srv->r && !srv->closed) {
        pthread_mutex_lock(&srv->r->lock);
        {
            kfs_room *room = kfs_room_find(srv->r, srv->room_id);
            if (room) kfs_srv_close_room_c(srv->r, room);
        }
        pthread_mutex_unlock(&srv->r->lock);
        srv->closed = 1;
        netref_release(srv->r);
        srv->r = NULL;
    }
    return 0;
}

// __gc：兜底关闭并释放引用（与 close 幂等）
static int kfs_srv_gc(lua_State *L)
{
    kfs_srvroom_t *srv = check_srvroom(L, 1);

    if (srv->r) {
        if (!srv->closed) {
            pthread_mutex_lock(&srv->r->lock);
            {
                kfs_room *room = kfs_room_find(srv->r, srv->room_id);
                if (room) kfs_srv_close_room_c(srv->r, room);
            }
            pthread_mutex_unlock(&srv->r->lock);
        }
        netref_release(srv->r);
        srv->r = NULL;
    }
    return 0;
}

//=====================================================================
// 模块入口
//=====================================================================
static const luaL_Reg kfs_module[] = {
    { "new",    kfs_new },
    { NULL, NULL }
};

// 必须对 Lua 可见：Makefile 用 -fvisibility=hidden，若不显式导出，
// Lua 用 dlsym 查找 luaopen_kcpframesynclib 会失败（undefined symbol）。
#if defined(__GNUC__)
#define KFS_API __attribute__((visibility("default")))
#else
#define KFS_API
#endif

KFS_API int luaopen_kcpframesynclib(lua_State *L)
{
    luaL_newlib(L, kfs_module);
    return 1;
}
