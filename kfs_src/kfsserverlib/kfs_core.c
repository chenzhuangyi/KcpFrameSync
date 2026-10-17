//=====================================================================
// kfs_core.c — 服务器库底层逻辑（不依赖 Lua）
//   netref 句柄管理 / 多玩家房间收集转发 / 驱动线程 / 服务器房间 C 层
//   结构定义见 kfs_server.h；公共工具见 common/kfs_common.h。
//=====================================================================
#include "kfs_server.h"

kfs_netref *netref_open(int port, uint32_t frame_interval_ms)
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

kfs_netref *netref_addref(kfs_netref *r)
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
void netref_close(kfs_netref *r)
{
    if (r == NULL || r->closed)
        return;
    netref_stop_drive(r);
}

// 释放一个引用；归零时关闭端口、停驱动线程并释放结构
void netref_release(kfs_netref *r)
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
kfs_room *kfs_room_find(kfs_netref *r, uint32_t room_id)
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


static int kfs_room_check_token(kfs_room *room, uint32_t conv, uint32_t token)
{
    print_log(" JOIN 检验 token conv=%u, token=%u, ptoken_count=%d\n", conv, token, (int)room->ptoken_count);
    int i;
    for (i = 0; i < (int)room->ptoken_count; i++) {
        print_log(" JOIN token 匹配 i=%d, iconv=%u, itoken=%u\n", i, room->ptokens[i].conv, room->ptokens[i].token);
        if (room->ptokens[i].conv == conv){
            print_log(" JOIN token 匹配 conv=%u, token=%u\n", conv, token);
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
    print_log(" kfs_srv_handle_join room_id=%d, conv=%u, token=%u\n", room_id, conv, token);
    kfs_room *room = kfs_room_find(r, room_id);
    char ack[5];
    uint32_t m = KFS_HANDSHAKE_MAGIC;
    uint8_t result;
    int rc;

    if (room == NULL) {
        print_log(" kfs_srv_handle_join 房间不存在 room_id=%d, conv=%u, token=%u\n", room_id, conv, token);
        result = 1;                               // 房间不存在
    } else if (!kfs_room_check_token(room, conv, token)) {
        print_log(" kfs_srv_handle_join token 错误 room_id=%d, conv=%u, token=%u\n", room_id, conv, token);
        result = 5;                               // token 错误（鉴权失败）
    } else {
        rc = kfs_srv_add_player_c(r, room, conv, peer);   // 登记玩家并建立连接
        result = (rc == 0) ? 0                    // 成功
               : (rc == -1) ? 2                   // 不在房间
               : (rc == -2) ? 3                   // conv 已在房间
               : 4;                               // 其他

        print_log(" kfs_srv_handle_join result=%d room_id=%d, conv=%u, token=%u\n", result, room_id, conv, token);
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
            memcpy(&conv, buf, 4);              // KCP 包头 4 字节即 conv（host order）
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

        if (r->rcount > 0 || r->room_count > 0) {
            // 每个驱动 tick：各房间最多推进一帧（帧率节拍）
            for (i = 0; i < r->room_count; i++)
                r->rooms[i].tick_done = 0;
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
                        print_log(" 玩家 conv=%u 掉线（席位保留，可重连）\n", pl->conv);
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
int kfs_srv_add_player_c(kfs_netref *r, kfs_room *room, uint32_t conv,
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
    print_log(" kfs_srv_add_player_c 建立/更新连接 conv=%d, room_id=%u\n", conv, room->room_id);
    return 0;
}

// 主动移除玩家（业务层清理）：释放其 KCP（如有）、收缩成员数组。
//   任何占位（INIT/ONLINE/LEFT）都可被移除；返回 0 成功；-1 不在房间。
int kfs_srv_remove_player_c(kfs_netref *r, kfs_room *room,
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

// 关闭房间（调用方须已持有 r->lock）：释放全部玩家 KCP，从房间表移除。
//   房间成员占位随房间一起销毁；玩家需重新 join 才能重进。
void kfs_srv_close_room_c(kfs_netref *r, kfs_room *room)
{
    int i;
    int idx = (int)(room - r->rooms);

    // 释放房间内全部玩家的 KCP 会话
    for (i = 0; i < (int)room->player_count; i++)
        if (room->players[i].kcp) {
            ikcp_release(room->players[i].kcp);
            room->players[i].kcp = NULL;
        }
    // 从房间表移除并收缩（末位填补），修正后续房间的 room_index
    if (idx != r->room_count - 1)
        memmove(&r->rooms[idx], &r->rooms[idx + 1],
                (size_t)(r->room_count - idx - 1) * sizeof(kfs_room));
    r->room_count--;
    for (i = idx; i < r->room_count; i++) {
        int j;
        for (j = 0; j < (int)r->rooms[i].player_count; j++)
            r->rooms[i].players[j].room_index = i;
    }
}
