//=====================================================================
// kfs_core.c — 服务器库底层逻辑（不依赖 Lua）
//   netref 句柄管理 / 多玩家房间收集转发 / 驱动线程 / 服务器房间 C 层
//   只能初始化一次，重复初始化会报错
//   结构定义见 kfs_server.h；公共工具见 common/kfs_common.h。
//=====================================================================
#include "kfs_server.h"
#include "uthash/src/uthash.h"

/* ---- 进程级单例 ---- */
static kfs_netref *g_netref = NULL;
static pthread_mutex_t g_init_lock = PTHREAD_MUTEX_INITIALIZER;


/* 内部：真正构造 netref（原 netref_open 的主体，抽出来） */
static kfs_netref *netref_new(int port, uint32_t frame_interval_ms)
{
    kfs_netref *r = calloc(1, sizeof(*r));
    if (!r) return NULL;

    r->net = net_open(port);           /* 你的 net.c 端口绑定 */
    if (!r->net) { free(r); return NULL; }

    r->frame_interval_ms = frame_interval_ms;
    r->refs = 1;                       /* 连接那份 */
    r->closed = 0;
    r->drunning = 1;
    r->djoined = 0;

    pthread_mutex_init(&r->lock, NULL);

    /* routes / rooms / events 按需 calloc 初始化（略，按你原逻辑） */

    /* 启动驱动线程 */
    if (pthread_create(&r->dthread, NULL, kfs_drive_thread, r) != 0) {
        net_close(r->net);
        pthread_mutex_destroy(&r->lock);
        free(r);
        return NULL;
    }
    return r;
}


/* ---- 公开 API ---- */
/* 单例 open：重复调用返回 NULL */
kfs_netref *netref_init(int port, uint32_t frame_interval_ms)
{
    pthread_mutex_lock(&g_init_lock);
    if (g_netref) {
        pthread_mutex_unlock(&g_init_lock);
        return NULL;          /* 已开，拒绝重复初始化 */
    }
    kfs_netref *r = netref_new(port, frame_interval_ms);
    if (r) g_netref = r;
    pthread_mutex_unlock(&g_init_lock);
    return r;
}

/* 取单例（供 kfs_init 复用，不增加 refs） */
kfs_netref *netref_get(void)
{
    pthread_mutex_lock(&g_init_lock);
    kfs_netref *r = g_netref;
    pthread_mutex_unlock(&g_init_lock);
    return r;
}

/* 显式关闭（主线程调用）：stop + join + 清单例 */
void netref_close(kfs_netref *r)
{
    int i, j;
    if (!r) return;

    // 第一步：先锁r自身锁，发停止信号，保证驱动线程能退出
    pthread_mutex_lock(&r->lock);
    r->drunning = 0;
    r->closed = 1;
    pthread_mutex_unlock(&r->lock);

    // 第二步：等待驱动线程退出（线程退出后，不会再碰r->lock）
    if (!r->djoined) {
        pthread_join(r->dthread, NULL);
        r->djoined = 1;
    }

    // 第三步：现在驱动线程已经退出，不会再访问r资源，再拿全局锁摘除单例
    pthread_mutex_lock(&g_init_lock);
    if (g_netref == r) {
        g_netref = NULL;
    }
    pthread_mutex_unlock(&g_init_lock);

    // 第四步：现在安全持有r，遍历释放所有KCP（此时无线程竞争r里面的房间/kcp）
    pthread_mutex_lock(&r->lock);
    {
        kfs_room *room, *rtmp;
        kfs_plink *lk, *ltmp;
        for (room = r->rooms; room != NULL; room = rtmp) {
            rtmp = room->hh.next;
            for (j = 0; j < (int)room->player_count; j++) {
                kfs_player *pl = &room->players[j];
                if (pl->kcp) { ikcp_release(pl->kcp); pl->kcp = NULL; }
            }
            HASH_DEL(r->rooms, room);
            free(room);
        }
        r->rooms = NULL;
        r->room_count = 0;
        // conv 索引节点统一收尾释放
        for (lk = r->pl_index; lk != NULL; lk = ltmp) {
            ltmp = lk->hh.next;
            free(lk);
        }
        r->pl_index = NULL;
    }
    for (i = 0; i < r->rcount; i++)
        if (r->routes[i].kcp) ikcp_release(r->routes[i].kcp);
    if (r->net) { net_close(r->net); r->net = NULL; }
    pthread_mutex_unlock(&r->lock);

    // 第五步：释放动态内存、销毁mutex
    free(r->routes);  r->routes = NULL;  r->rcount = r->rcap = 0;
    free(r->events);  r->events = NULL;  r->ecount = r->ecap = 0;

    pthread_mutex_destroy(&r->lock);
    free(r);
}

/* 引用计数（多 conn 共享同一 netref 时用） */
kfs_netref *netref_addref(kfs_netref *r)
{
    if (!r) return NULL;
    pthread_mutex_lock(&r->lock);
    r->refs++;
    pthread_mutex_unlock(&r->lock);
    return r;
}

void netref_release(kfs_netref *r)
{
    if (!r) return;
    pthread_mutex_lock(&r->lock);
    if (r->refs > 0)
        --r->refs;
    pthread_mutex_unlock(&r->lock);
}


// 注册一个房间到路由表；成功返回 0，conv 冲突返回 -1
// 调用方须已持有 r->lock
int netref_route_add(kfs_netref *r, IUINT32 conv, ikcpcb *kcp)
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
void netref_route_del(kfs_netref *r, ikcpcb *kcp)
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
ikcpcb *netref_route_find(kfs_netref *r, IUINT32 conv)
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
// 按 conv 查找在线玩家会话（驱动线程收包路由用）。O(1)：uthash 全局索引
// pl_index。不变式：仅 state==ONLINE 的玩家在表内、conv 全局唯一——JOIN 升级/
// 重连挂表，转房踢除、remove、关房、netref 关闭均摘表（见 pl_index_* 辅助）。
static kfs_player *kfs_find_player(kfs_netref *r, uint32_t conv)
{
    kfs_plink *lk = NULL;
    if (r == NULL)
        return NULL;
    HASH_FIND(hh, r->pl_index, &conv, sizeof(conv), lk);
    return lk ? lk->pl : NULL;
}

// 按 room_id 查找房间。O(1)：rooms 为 uthash（room_id → 堆分配 kfs_room*）。
kfs_room *kfs_room_find(kfs_netref *r, uint32_t room_id)
{
    kfs_room *room = NULL;
    if (r == NULL)
        return NULL;
    HASH_FIND(hh, r->rooms, &room_id, sizeof(room_id), room);
    return room;
}

//---------------------------------------------------------------------
// conv→玩家 全局索引维护（调用方须持有 r->lock）
//   入表前该 conv 不应已在表内；冲突（跨房重复 conv）由调用方语义保证不发生
//---------------------------------------------------------------------
static int kfs_pl_index_add(kfs_netref *r, kfs_player *pl)
{
    kfs_plink *lk;
    HASH_FIND(hh, r->pl_index, &pl->conv, sizeof(pl->conv), lk);
    if (lk != NULL) {
        if (lk->pl == pl)
            return 0;               // 已在表内且指向同一槽：幂等
        return -1;                  // conv 冲突：不同玩家占用同一 conv
    }
    lk = (kfs_plink *)malloc(sizeof(*lk));
    if (lk == NULL)
        return -1;
    lk->conv = pl->conv;
    lk->pl = pl;
    HASH_ADD(hh, r->pl_index, conv, sizeof(uint32_t), lk);
    return 0;
}

static void kfs_pl_index_del(kfs_netref *r, uint32_t conv)
{
    kfs_plink *lk = NULL;
    HASH_FIND(hh, r->pl_index, &conv, sizeof(conv), lk);
    if (lk != NULL) {
        HASH_DEL(r->pl_index, lk);
        free(lk);
    }
}

// 组装广播帧：payload = 逐玩家 [conv 4B][len 2B][data...] + 末尾 4B FNV 对账哈希
// 直接写入调用方 out 缓冲（预留帧头位），热路径零堆分配。返回总字节数；失败 -1。
static int kfs_room_build_broadcast(kfs_room *room, char *out, int out_cap)
{
    char *p, *body;
    uint32_t h;
    uint16_t dl;
    int i, len;

    if (out == NULL || out_cap < KFS_FRAME2_HEADER_SIZE)
        return -1;
    p = body = out + KFS_FRAME2_HEADER_SIZE;
    for (i = 0; i < (int)room->player_count; i++) {
        if (!KFS_PLAYER_ONLINE(&room->players[i]))
            continue;
        len = room->input_len[i];
        if (len > KFS_FRAME_INPUT_MAX)
            continue;             // 防御：异常长度
        // 本条需 6B+data，末尾还要留 4B 哈希；放不下即整帧放弃（等价旧版 malloc 失败）
        if (out_cap - (int)(p - out) < 6 + len + 4)
            return -1;
        memcpy(p, &room->players[i].conv, 4); p += 4;
        dl = (uint16_t)len;
        memcpy(p, &dl, 2); p += 2;
        if (len > 0) {
            memcpy(p, room->input_buf[i], (size_t)len);
            p += len;
        }
    }
    len = (int)(p - body);
    h = frame_fnv1a(body, len);
    memcpy(p, &h, 4); p += 4;
    len = (int)(p - body);                        // payload（含末尾哈希）
    if (len > KFS_FRAME_MAX_DATA)
        return -1;                                // 超 2B 长度域（32 人×518B 远不到，防御性）
    out[0] = (char)KFS_CMD_BROADCAST;
    memcpy(out + 1, &room->collecting_fid, 4);
    dl = (uint16_t)len;
    memcpy(out + 5, &dl, 2);
    return KFS_FRAME2_HEADER_SIZE + len;
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
    // 用 recv_mask 位图判集齐：活跃玩家的对应位必须都已置 1。
    // 注意：位图只在"输入到达时刻恰等于 collecting_fid"时由收包泵置位，
    // 而客户端带超前窗口（advance_frames），输入通常先于 collecting 到达、
    // 只躺在 pend 缓冲里。故此处对未置位的玩家先自查 pend（与 rejoin 路径
    // 同款自愈）；否则帧永远只能靠 timeout 兜底携带旧输入空转。
    // 掉线玩家（offline=1）不参与判定，广播时用其 input_buf 最近输入填充。
    for (i = 0; i < (int)room->player_count; i++) {
        kfs_player *pl = &room->players[i];
        if (!KFS_PLAYER_ACTIVE(pl))
            continue;
        if (!(room->recv_mask & (1U << pl->slot_index))) {
            kfs_pframe *cur = &pl->pend[fid % KFS_PLAYER_BUF];
            if (pl->slot_index >= 0 && pl->slot_index < KFS_ROOM_MAX_PLAYER &&
                cur->valid && cur->fid == fid)
                room->recv_mask |= (1U << pl->slot_index);  // 输入早已到达：补位
            else
                return;                             // 本帧尚缺该玩家输入，等下个 tick
        }
    }
    LOG_DEBUGF("kfs_room_try_advance room_id=%d fid=%u", room->room_id, fid);
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
        if (KFS_PLAYER_ONLINE(&room->players[i])){
            LOG_DEBUGF("kfs_room_try_advance room_id=%d conv=%d fid=%u", room->room_id, room->players[i].conv, fid);
            ikcp_send(room->players[i].kcp, pkt, total);   // → net_send_to
        }
            

    room->collecting_fid++;                     // 推进帧号
    room->collect_begin_ms = kfs_now_ms();
    room->recv_mask = 0;                        // 进入下一帧：清空收集位图
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
            pl->state_hash[fid % KFS_PLAYER_BUF] == 0) {
            if (pl->offline || pl->fid_last + KFS_PLAYER_BUF < fid)
                continue;               // 已掉线/明显滞后：本帧对账豁免
            return;                     // 正常在线但未报：等下个 tick
        }
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

// 房间内所有「在线」玩家都 ready==1 时，向所有在线客户端广播 one-shot
// 「全部就绪/开战」帧，并把房间推入收集态 phase=1。
// 仅在驱动线程内调用（调用方已持有 r->lock）。
static void kfs_room_check_all_ready(kfs_netref *r, kfs_room *room)
{
    int i, online = 0;
    char pkt[KFS_FRAME_MAX_TOTAL];
    uint32_t start_fid;
    int ret;

    if (room == NULL || room->player_count == 0)
        return;
    if (room->phase == 1)                 // 已开战：不重复广播
        return;

    // 1. 就绪判定：所有在线玩家 ready 必须都为 1；掉线玩家不参与判定
    for (i = 0; i < (int)room->player_count; i++) {
        kfs_player *pl = &room->players[i];
        if (!(KFS_PLAYER_ONLINE(pl)||KFS_PLAYER_INIT(pl)))
            continue;                     // 掉线/无 KCP：不计入就绪集合
        online++;
        if (!pl->ready)
            return;                       // 仍有在线玩家未就绪：等其 ready 帧
    }
    if (online == 0)
        return;                           // 无在线玩家，无需开战

    // 2. 组装 one-shot 开战帧：起始帧号取当前 collecting_fid，无业务 payload
    start_fid = room->collecting_fid;
    char ack_buf[12];
    int ack_len = 0;
    uint64_t now = kfs_now_ms();               // 落临时变量再取址
    memcpy(ack_buf + ack_len, &now, 8); ack_len += 8;
    ret = frame_encode2(pkt, (int)sizeof(pkt), KFS_CMD_ALL_READY,
                        start_fid, ack_buf, 0);
    if (ret < 0)
        return;

    // 3. 广播给所有在线客户端（仅 ONLINE 且有 KCP 的会话）
    for (i = 0; i < (int)room->player_count; i++) {
        kfs_player *pl = &room->players[i];
        if (!KFS_PLAYER_ONLINE(pl))
            continue;
        LOG_DEBUGF("kfs_room_check_all_ready room_id=%d conv=%d start_fid=%u",
                   room->room_id, pl->conv, start_fid);
        ikcp_send(pl->kcp, pkt, ret);     // -> net_send_to
    }

    // 4. 进入开战/收集推帧状态：phase=1，重置本帧收集节拍与位图
    room->phase = 1;
    room->collecting_fid = 0;
    room->recv_mask = 0;
    memset(room->input_len, 0, sizeof(room->input_len));
    room->collect_begin_ms = kfs_now_ms();
    room->tick_done = 0;
}


// 从某玩家 KCP 会话解出完整帧，写入其超前输入缓冲（服务器侧驱动线程内调用）
static void kfs_player_recv_pump(kfs_netref *r, kfs_player *pl, uint64_t now_ms)
{
    char tmp[KFS_FRAME_MAX_TOTAL];
    int n;
    kfs_room *room;

    if (pl->room == NULL)
        return;
    room = pl->room;

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
            continue;
        }
        if (type == KFS_CMD_READY) {
            LOG_DEBUGF("kfs_player_recv_pump 客户端ready room_id=%d conv=%d fid=%u", room->room_id, pl->conv, fid);
            pl->ready = 1;                      // 客户端加载连接完毕，业务层可据此开战
            pl->last_active_ms = now_ms;
            // 
            char ack_buf[5];
            int sync_len = 0;
            ack_buf[sync_len++] = 1;            // 修复：原 memcpy(…,1,1) 把整数1当指针解引用
            // 类型自定义 KFS_CMD_REJOIN_SYNC = 9 加到enum
            char pkt[KFS_FRAME_MAX_TOTAL];
            int ret = frame_encode2(pkt, sizeof(pkt), KFS_CMD_READY_ACK, 0, ack_buf, sync_len);
            if (ret > 0) {
                ikcp_send(pl->kcp, pkt, ret);
            }

            // 检查是否全部就绪，全部就绪广播给所有在线客户端
            kfs_room_check_all_ready(r, room);

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
        if (dlen > KFS_FRAME_INPUT_MAX) {
            LOG_DEBUGF("drop oversized INPUT conv=%d fid=%d len=%d > %d",
                       pl->conv, fid, dlen, KFS_FRAME_INPUT_MAX);
            continue;                           // 超长输入：拒绝
        }
        if (fid > room->collecting_fid + KFS_PLAYER_BUF || fid < room->sync_fid)
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

        // 收齐本帧输入即在房间位图打点（供 try_advance O(1) 判集齐）
        if (fid == room->collecting_fid &&
            pl->slot_index >= 0 && pl->slot_index < KFS_ROOM_MAX_PLAYER)
            room->recv_mask |= (1U << pl->slot_index);
        LOG_DEBUGF("KFS_CMD_INPUT conv=%d fid=%d data=%.*s", pl->conv, fid, dlen, dlen > 0 ? data : "");
    }
}

// 可靠传输下慢玩家兜底：超过 timeout 未集齐，用最近完整帧输入广播推进，
// 避免死等。注：线上标准做法是判定掉线/降级，这里暂保留旧输入填充作 fallback。
static void kfs_room_timeout_tick(kfs_netref *r, kfs_room *room,
                                  uint64_t now_ms)
{
    if (room->phase != 1) 
        return;
    if (room->player_count == 0 || room->tick_done)
        return;
    if (now_ms - room->collect_begin_ms < room->timeout_ms)
        return;

    char pkt[KFS_FRAME_MAX_TOTAL];
    int total, i;
    
    LOG_DEBUGF("kfs_room_timeout_tick room_id=%d collecting_fid=%u", room->room_id, room->collecting_fid);
    // input_buf 保留最近一次集齐广播时的全员输入，直接以其推进
    total = kfs_room_build_broadcast(room, pkt, (int)sizeof(pkt));
    if (total < 0)
        return;
    for (i = 0; i < (int)room->player_count; i++)
        if (KFS_PLAYER_ONLINE(&room->players[i])){
            LOG_DEBUGF("kfs_room_timeout_tick room_id=%d conv=%d collecting_fid=%u", room->room_id, room->players[i].conv, room->collecting_fid);
            ikcp_send(room->players[i].kcp, pkt, total);
        }
        
    room->collecting_fid++;                     // 推进帧号
    room->collect_begin_ms = now_ms;
    room->recv_mask = 0;                        // 超时强推：位图随之作废重收
    room->tick_done = 1;                        // 本 tick 已推进一帧
}


// 登记一个玩家(conv)的会话 token（per-player 鉴权）。
//   已登记则覆盖；返回 0 成功；-1 表满。
int kfs_srv_set_player_token_c(kfs_room *room, uint32_t conv,
                                      uint32_t token)
{
    int i;
    for (i = 0; i < (int)room->ptoken_count; i++)
        if (room->ptokens[i].conv == conv) {
            room->ptokens[i].token = token;
            room->ptokens[i].valid = 1;
            return 0;
        }
    if (room->ptoken_count >= KFS_ROOM_MAX_PLAYER)
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
    LOG_DEBUGF(" kfs_srv_handle_join room_id=%d, conv=%u, token=%u", room_id, conv, token);
    kfs_room *room = kfs_room_find(r, room_id);
    char ack[5];
    uint32_t m = KFS_HANDSHAKE_MAGIC;
    uint8_t result = KFS_CODE_SUCCESS;
    int rc;

    if (room == NULL) {
        LOG_DEBUGF(" kfs_srv_handle_join 房间不存在 room_id=%d, conv=%u, token=%u", room_id, conv, token);
        result = KFS_CODE_ERROR_ROOM_NULL;                               // 房间不存在
    }

    if (result == KFS_CODE_SUCCESS) {
        // 处理token鉴权和建立/更新连接
        result = kfs_srv_add_player_c(r, room, conv, token, peer);
        if (result != KFS_CODE_SUCCESS) {
            LOG_DEBUGF("kfs_srv_handle_join add_player fail, code=%d room_id=%u conv=%u token=%u",
                result, room_id, conv, token);
        }
    }

    if (result != KFS_CODE_SUCCESS)
    {
        memcpy(ack, &m, 4);
        ack[4] = (char)result;
        int ret5 = net_send_to(r->net, peer, ack, 5);
        LOG_DEBUGF("JOIN_ACK 回包(失败) conv=%u peer=%s:%u sent=%d result=%u",
            conv, inet_ntoa(peer->sin_addr), ntohs(peer->sin_port), ret5,
            (unsigned)result);
    }
    
}

//---------------------------------------------------------------------
// 自动驱动线程
//   绑定端口成功后由 netref_init 启动。循环：
//     1. 判定路由表里是否存在有效房间（rcount > 0）。
//     2. 无房间：空转休眠（不调用任何 ikcp_*）。
//     3. 有房间：从网络队列按 conv 路由喂包，并对全部房间
//        ikcp_update()（驱动 send 侧 flush / recv 侧解包）。
//---------------------------------------------------------------------
void *kfs_drive_thread(void *arg)
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
        now = kfs_now_ms();
        // 收包：先识别裸 UDP JOIN 握手（带/不带 token），其余按 KCP conv 路由。
        // 收包必须放在“是否有房间”的判断之外：没有房间时也要回 JOIN 的
        // result=1，否则客户端只能等到 2s 超时，无法区分“服务器没跑”和“房间号错”。
        while ((rcv = net_recv(r->net, buf, KFS_NET_MAX_PKT, &from)) > 0) {
            IUINT32 m, conv;
            kfs_player *pl;
            if (rcv < 4)
                continue;                       // 过短，丢弃
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
            memcpy(&conv, buf, 4);
            pl = kfs_find_player(r, conv);
            if (pl) {
                // 仅"已掉线"时才从新源学习对端：在线态的对端由成功 JOIN 权威设定，
                // 防止旧链路超时前的残留杂包把 peer 反向改回旧地址造成下行抖动。
                if (KFS_PLAYER_ONLINE(pl) && pl->offline &&
                    (pl->peer.sin_addr.s_addr != from.sin_addr.s_addr ||
                     pl->peer.sin_port        != from.sin_port)) {
                    pl->peer    = from;     // ctx.peer 指向 &pl->peer，自动跟随
                    pl->offline = 0;        // 收到有效包即视为恢复
                    LOG_DEBUGF("conv=%u 由新源恢复 peer=%s:%u", conv,
                        inet_ntoa(from.sin_addr), ntohs(from.sin_port));
                }
                ikcp_input(pl->kcp, buf, (long)rcv);
                kfs_player_recv_pump(r, pl, (uint64_t)now);
            } else {
                ikcpcb *kcp = netref_route_find(r, conv);
                if (kcp)
                    ikcp_input(kcp, buf, (long)rcv);
                // 未匹配的 conv：丢弃（不属于本端口任何房间）
            }
        }

        if (r->rcount > 0 || r->room_count > 0) {
            kfs_room *room, *rtmp;
            // 每个驱动 tick：各房间最多推进一帧（帧率节拍）
            for (room = r->rooms; room != NULL; room = room->hh.next)
                room->tick_done = 0;
            // update 点对点房间
            for (i = 0; i < r->rcount; i++)
                ikcp_update(r->routes[i].kcp, now);
            // update 多玩家房间 + 写缓冲 + 每 tick 尝试推进一帧 + 对账 + 超时兜底
            for (room = r->rooms; room != NULL; room = room->hh.next) {
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
            for (room = r->rooms; room != NULL; room = room->hh.next) {
                int j;
                if (room->idle_timeout_ms == 0)
                    continue;
                for (j = 0; j < (int)room->player_count; j++) {
                    kfs_player *pl = &room->players[j];
                    if (KFS_PLAYER_ONLINE(pl) && !pl->offline &&
                        (uint64_t)now - pl->last_active_ms > room->idle_timeout_ms) {
                        pl->offline = 1;        // 标记掉线：席位保留，等待重连
                        LOG_DEBUGF("玩家 conv=%u 掉线（席位保留，可重连）", pl->conv);
                    }
                }
            }
            // 房间 TTL：超过最长保留时间自动销毁（冗余清理僵尸房间）。
            //   显式快照 next 再销毁当前房间（close_room 会 HASH_DEL 摘链）。
            for (room = r->rooms; room != NULL; room = rtmp) {
                rtmp = room->hh.next;
                if (room->ttl_ms != 0 &&
                    (uint64_t)now - room->create_ms > room->ttl_ms)
                    kfs_srv_close_room_c(r, room);
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

int kfs_output(const char *buf, int len, ikcpcb *kcp, void *user)
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
// 服务器多玩家房间（收集转发）—— 内部 C 层
//   调用方须已持有 r->lock。
//=====================================================================
// 创建多玩家房间（调用方须已持有 r->lock）。
//   init_convs / init_count：初始玩家列表。创建时把初始玩家预登记为 INIT
//     占位（无连接），等客户端 JOIN 时升级为 ONLINE；初始玩家掉线不移除
//     （席位保留等待重连）。
//   校验：init_count 小于等于0 || 超过 KFS_ROOM_MAX_PLAYER 返回 -3；room_id 重复 -1；内存不足 -2。
int kfs_srv_new_room_c(kfs_netref *r, uint32_t room_id,
                              uint64_t timeout_ms, uint32_t token,
                              uint64_t ttl_ms, uint64_t idle_timeout_ms,
                              const uint32_t *init_convs, int init_count)
{
    kfs_room *nr, *or;
    int i, k;

    if (init_count <= 0 || init_count > KFS_ROOM_MAX_PLAYER)
        return -3;                              // 初始玩家数异常
    if (kfs_room_find(r, room_id) != NULL)
        return -1;                              // room_id 已存在

    // 房间改为堆分配单对象 + uthash 索引：地址稳定（玩家可直接持 room 指针），
    // 且不再有 realloc 搬移导致的悬挂与 room_index 批量修正。
    nr = (kfs_room *)calloc(1, sizeof(kfs_room));
    if (nr == NULL)
        return -2;                              // 内存不足
    nr->room_id = room_id;
    HASH_ADD(hh, r->rooms, room_id, sizeof(uint32_t), nr);
    r->room_count++;

    // 对每一个要加入新房间的初始conv，遍历全房间表，把旧房间里的该玩家踢为LEFT。
    //   注意：这里必须覆盖 INIT 占位（尚未 JOIN、不在 pl_index 内），
    //   故保留全表遍历——建房间是低频操作，不在每帧热路径。
    for (i = 0; i < init_count; i++) {
        uint32_t chk_conv = init_convs[i];
        int found_old = 0;
        for (or = r->rooms; or != NULL && !found_old; or = or->hh.next) {
            if (or == nr)
                continue;                       // 跳过自己
            for (k = 0; k < (int)or->player_count; k++) {
                kfs_player *opl = &or->players[k];
                if (opl->conv == chk_conv && opl->state != KFS_PSTATE_LEFT) {
                    if (opl->kcp) {
                        ikcp_release(opl->kcp);
                        opl->kcp = NULL;
                        kfs_pl_index_del(r, opl->conv);   // 曾是 ONLINE：摘索引
                    }
                    opl->state = KFS_PSTATE_LEFT;
                    opl->offline = 1;
                    opl->room = NULL;
                    memset(opl->pend, 0, sizeof(opl->pend));
                    found_old = 1;
                    break;
                }
            }
        }
    }

    uint32_t room_seed = kfs_rng_next();
    if (room_seed == 0)
    {
        LOG_DEBUGF("kfs_srv_new_room_c 初始化随机因子失败");
        room_seed = (uint32_t)kfs_now_ms() | 1u;   // 兜底非零，避免 seed 恒为 0
    }

    nr->room_seed = room_seed;          // 关键：把随机因子持久化，重连才能回滚 RNG
    nr->max_player = 0;                // 设定的最大玩家数（满员判定用）
    nr->init_count = init_count;                // 初始玩家预登记数
    nr->player_count = init_count;
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
        pl->conv = init_convs[i];
        pl->state = KFS_PSTATE_INIT;
        pl->room = nr;
        pl->slot_index = i;
        pl->last_active_ms = kfs_now_ms();      // 创建起即计活跃，避免立即被判掉线
    }
    nr->ptoken_count = 0;
    return 0;
}

// JOIN 登记：把玩家(conv)加入房间并建立连接（调用方须已持有 r->lock）。
//   目标房间内按 conv 复用槽位：
//        INIT（初始登记）      → 升级 ONLINE（首连）
//        ONLINE 且掉线(offline)→ 恢复 ONLINE（掉线重连）
//        ONLINE 且在线         → 返回 -2（重复 JOIN）
//        LEFT                  → 复活为 ONLINE（转房后回归）
//      无该 conv 槽位          → 末尾追加新成员 ONLINE（join 列表）
//   3) 建立/更新 KCP 会话与对端地址（掉线重连沿用已有 KCP，仅更新对端）。
//   返回 0 成功；-1 不在房间中；-2 conv 已在线在房；-3 KCP 创建失败。
int kfs_srv_add_player_c(kfs_netref *r, kfs_room *room, uint32_t conv, uint32_t token,
                         const struct sockaddr_in *peer)
{
    kfs_player *pl = NULL;
    int i, j;
    int has_ptoken = 0;                 /* 该 conv 是否登记了专属 token */
    /* 鉴权：per-player token 优先，未登记则回退房间级 token */
    for (i = 0; i < (int)room->ptoken_count; i++) {
        if (room->ptokens[i].conv == conv) {
            has_ptoken = 1;
            if (!room->ptokens[i].valid || room->ptokens[i].token != token)
                return KFS_CODE_ERROR_JOIN_TOKEN;     /* 配了专属 token 但本次不符 */
            break;
        }
    }
    if (!has_ptoken) {
        /* 未登记专属 token：回退房间级 token（room_token==0 表示不校验） */
        if (room->room_token != 0 && room->room_token != token)
            return KFS_CODE_ERROR_JOIN_TOKEN;
    }
    /* 2) 目标房内按 conv 找槽位；不在初始名单一律拒绝（不再追加） */
    for (i = 0; i < (int)room->player_count; i++) {
        if (room->players[i].conv == conv) {
            pl = &room->players[i];
            break;
        }
    }
    if (pl == NULL)
        return KFS_CODE_ERROR_NOT_IN_ROOM;          /* 名单外：拒绝 */

    if (pl->state == KFS_PSTATE_LEFT)
    {
        return KFS_CODE_ERROR_NOT_IN_ROOM;          /* 玩家主动退出 */
    }
    
    // 已经在线且未掉线 → 拒绝重连握手，防止误杀正常KCP
    if (pl->state == KFS_PSTATE_ONLINE && !pl->offline)
    {
        // 源地址完全一致才是真重复握手 → 拒绝
        if (pl->peer.sin_addr.s_addr == peer->sin_addr.s_addr &&
            pl->peer.sin_port        == peer->sin_port)
            return KFS_CODE_ERROR_ALREADY_ONLINE;
        // 源地址变化 → 判定换网络重连，放行：下方释放旧KCP、用新peer重建会话
        LOG_DEBUGF("换网络重连 conv=%u 新peer:%s:%u", conv,
            inet_ntoa(peer->sin_addr), ntohs(peer->sin_port));
    }

    int is_rejoin = (pl->kcp != NULL);   // 放 ikcp_release 之前先记录：原来是否已有连接

    /* 3) 建立/更新连接：INIT/LEFT/掉线重连 重建 KCP */
    if (pl->kcp)
        ikcp_release(pl->kcp);
    pl->peer = *peer;
    pl->ctx.r = r;
    pl->ctx.peer = &pl->peer;
    pl->kcp = ikcp_create(conv, (void *)&pl->ctx);
    if (!pl->kcp) return KFS_CODE_ERROR_CREATE_KCPCB;
    ikcp_setoutput(pl->kcp, kfs_output);
    ikcp_nodelay(pl->kcp, 1, 10, 2, 1);
    
    pl->state   = KFS_PSTATE_ONLINE;
    pl->offline = 0;
    pl->ready   = 0;
    pl->room    = room;
    pl->last_active_ms = kfs_now_ms();
    // 挂入 conv 全局索引（幂等：掉线重连同指针直接返回）；
    //   若同 conv 已挂在别的玩家槽上（跨房 conv 冲突异常），拒绝以免路由二义。
    if (kfs_pl_index_add(r, pl) != 0) {
        LOG_DEBUGF("conv=%u 索引冲突，JOIN 拒绝", conv);
        ikcp_release(pl->kcp);
        pl->kcp = NULL;
        pl->state = KFS_PSTATE_INIT;
        pl->room = NULL;
        return KFS_CODE_ERROR_NOT_IN_ROOM;
    }
    // if (room->phase == 0)
    //     kfs_room_push_ready_event(r, room);   // 业务据此重算就绪、决定 start()

    // 新增1+2 合并：先无条件清本玩家收集位，再做窗口裁剪，
    // 最后若 collecting_fid 帧输入仍有效则重新置位，保持位图与 pend 一致。
    if (pl->slot_index >= 0 && pl->slot_index < KFS_ROOM_MAX_PLAYER)
        room->recv_mask &= ~(1U << pl->slot_index);

    uint32_t base_fid    = room->sync_valid ? room->sync_fid : 0;
    uint32_t max_allow   = room->collecting_fid + KFS_PLAYER_BUF;
    int b;
    for (b = 0; b < KFS_PLAYER_BUF; b++) {
        kfs_pframe *pf = &pl->pend[b];
        if (pf->valid && (pf->fid < base_fid || pf->fid > max_allow))
            pf->valid = 0;
    }
    if (pl->slot_index >= 0 && pl->slot_index < KFS_ROOM_MAX_PLAYER) {
        kfs_pframe *cur = &pl->pend[room->collecting_fid % KFS_PLAYER_BUF];
        if (cur->valid && cur->fid == room->collecting_fid)
            room->recv_mask |= (1U << pl->slot_index);   // 本帧输入确实还在 → 补置位
    }
    
    uint32_t sfid = room->sync_valid ? room->sync_fid : 0;
    uint32_t cfid = room->collecting_fid;

    uint8_t result = KFS_CODE_SUCCESS;
    uint32_t m = KFS_HANDSHAKE_MAGIC;
    char ack[32];                       // 放大：实际写 21 字节，留余量
    uint8_t ack_len = 0;
    // JOIN 应答：[magic 4B][result 1B][frame_interval_ms 4B][room_seed 4B][server_time_ms 8B] = 21B
    memcpy(ack + ack_len, &m, 4); ack_len += 4;
    ack[ack_len++] = (char)result;
    uint32_t fi = r->frame_interval_ms;
    memcpy(ack + ack_len, &fi, 4); ack_len += 4;
    uint32_t seed = room->room_seed;           // 注意是 room 不是 r，且 4 字节
    memcpy(ack + ack_len, &seed, 4); ack_len += 4;
    uint64_t now = kfs_now_ms();               // 落临时变量再取址
    memcpy(ack + ack_len, &now, 8); ack_len += 8;
    int ack_ret = net_send_to(r->net, peer, ack, ack_len);
    LOG_DEBUGF("JOIN_ACK 回包 conv=%u peer=%s:%u sent=%d result=%u acklen=%u",
        conv, inet_ntoa(peer->sin_addr), ntohs(peer->sin_port), ack_ret,
        (unsigned)result, (unsigned)ack_len);
    if (is_rejoin) {//暂时不启用
        // // ==========【新增3：给这个重连玩家下发锚点同步包（业务快照包，这里发锚控制帧）】==========
        // // 业务层可以在这里打包【当前房间所有玩家状态快照】一起下发
        // char sync_buf[128];
        // int sync_len = 0;
        // memcpy(sync_buf + sync_len, &sfid, 4);              sync_len += 4;
        // memcpy(sync_buf + sync_len, &cfid, 4);              sync_len += 4;
        // memcpy(sync_buf + sync_len, &room->room_seed, 4);          sync_len += 4; // 新增
        // memcpy(sync_buf + sync_len, &r->frame_interval_ms, 4);     sync_len += 4; // 新增
        // // 类型自定义 KFS_CMD_REJOIN_SYNC = 9 加到enum
        // char pkt[KFS_FRAME_MAX_TOTAL];
        // int ret = frame_encode2(pkt, sizeof(pkt), 9/*KFS_CMD_REJOIN_SYNC*/, cfid, sync_buf, sync_len);
        // if (ret > 0) {
        //     ikcp_send(pl->kcp, pkt, ret);
        // }
    
        // // ==========【新增4：房间广播：通知其他在线玩家：该conv重连上线】==========
        // // 业务层收到这个广播，同步该玩家角色、buff、位置
        // char evt_buf[32];
        // int evt_len = 0;
        // memcpy(evt_buf + evt_len, &conv, 4); evt_len +=4;
        // for (int j = 0; j < (int)room->player_count; j++) {
        //     kfs_player *other = &room->players[j];
        //     if (KFS_PLAYER_ONLINE(other) && other != pl) {
        //         int nret = frame_encode2(pkt, sizeof(pkt), 10/*KFS_CMD_PLAYER_REJOIN*/, cfid, evt_buf, evt_len);
        //         if (nret > 0) ikcp_send(other->kcp, pkt, nret);
        //     }
        // }
    }

    LOG_DEBUGF("player join success room_id=%u, conv=%u, is_rejoin=%d, sync_fid=%u, collecting_fid=%u", room->room_id, conv, is_rejoin, sfid, cfid);
    return KFS_CODE_SUCCESS;
}


// 主动移除玩家（业务层清理）：释放其 KCP（如有）、收缩成员数组。
//   任何占位（INIT/ONLINE/LEFT）都可被移除；返回 0 成功；-1 不在房间。
//   conv 查找优先走全局索引（ONLINE），未命中回退本房数组扫描（INIT/LEFT 占位）。
int kfs_srv_remove_player_c(kfs_netref *r, kfs_room *room,
                                   uint32_t conv)
{
    kfs_player *pl = NULL;
    kfs_plink *lk = NULL;
    int i, last;

    HASH_FIND(hh, r->pl_index, &conv, sizeof(conv), lk);
    if (lk != NULL && lk->pl != NULL && lk->pl->room == room) {
        pl = lk->pl;                            // ONLINE：O(1) 命中
    } else {
        for (i = 0; i < (int)room->player_count; i++) {
            if (room->players[i].conv == conv) { pl = &room->players[i]; break; }
        }
        lk = NULL;                              // 回退路径无索引节点
    }
    if (pl == NULL)
        return -1;                              // conv 不在房间
    i = pl->slot_index;
    if (pl->kcp) {
        ikcp_release(pl->kcp);
        pl->kcp = NULL;
    }
    if (lk != NULL) {                           // 摘除被移除 conv 的路由
        HASH_DEL(r->pl_index, lk);
        free(lk);
    }
    last = (int)room->player_count - 1;
    if (i != last) {
        room->players[i] = room->players[last];
        // 关键：拷贝完结构体，修复peer自引用指针
        room->players[i].ctx.peer = &room->players[i].peer;
        room->players[i].slot_index = i;
        // 被搬移的玩家若 ONLINE，其索引节点要指向新槽位
        if (KFS_PLAYER_ONLINE(&room->players[i])) {
            kfs_plink *mk = NULL;
            uint32_t mconv = room->players[i].conv;
            HASH_FIND(hh, r->pl_index, &mconv, sizeof(mconv), mk);
            if (mk != NULL)
                mk->pl = &room->players[i];
        }
    }

    memset(&room->players[last], 0, sizeof(kfs_player));
    room->players[last].room = NULL;
    room->players[last].slot_index = -1;
    room->player_count--;
    // 离房后本帧收集作废，等下一帧重新收集
    room->recv_mask = 0;
    room->collect_begin_ms = kfs_now_ms();
    memset(room->input_len, 0, sizeof(room->input_len));
    return 0;
}

// 关闭房间（调用方须已持有 r->lock）：释放全部玩家 KCP，从房间哈希移除并销毁。
//   房间成员占位随房间一起销毁；玩家需重新 join 才能重进。
void kfs_srv_close_room_c(kfs_netref *r, kfs_room *room)
{
    int i;

    // 释放房间内全部玩家的 KCP 会话；ONLINE 玩家同步摘除 conv 路由索引
    for (i = 0; i < (int)room->player_count; i++) {
        kfs_player *pl = &room->players[i];
        if (pl->kcp) {
            ikcp_release(pl->kcp);
            pl->kcp = NULL;
        }
        if (KFS_PLAYER_ONLINE(pl))
            kfs_pl_index_del(r, pl->conv);
        pl->room = NULL;
    }
    // 从房间哈希摘链并销毁堆对象（地址稳定，无需再修正其他房间的 room_index）
    HASH_DEL(r->rooms, room);
    if (r->room_count > 0)
        r->room_count--;
    free(room);
}
