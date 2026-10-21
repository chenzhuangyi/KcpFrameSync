//=====================================================================
// kfs_client_lua.c — 客户端库 Lua 绑定层
//   fs.new / setpeer / join / 输入·就绪·广播·状态·追帧 / 心跳 / 参数 / 生命周期
//   底层逻辑见 kfs_client_core.c；结构定义见 kfs_client.h。
//=====================================================================
#include "kfs_client.h"

static kfs_client *check_client(lua_State *L, int idx)
{
    return (kfs_client *)luaL_checkudata(L, idx, "kfsclientlib");
}

//---------------------------------------------------------------------
// 方法前向声明（metatable 在工厂函数中引用，须先声明后使用）
//---------------------------------------------------------------------
static int kfs_setpeer(lua_State *L);
static int kfs_join(lua_State *L);
static int kfs_send_input(lua_State *L);
static int kfs_send_ready(lua_State *L);
static int kfs_recv_broadcast(lua_State *L);
static int kfs_report_state(lua_State *L);
static int kfs_catch_up(lua_State *L);
static int kfs_set_heartbeat(lua_State *L);
static int kfs_nodelay(lua_State *L);
static int kfs_wndsize(lua_State *L);
static int kfs_setmtu(lua_State *L);
static int kfs_closed(lua_State *L);
static int kfs_close(lua_State *L);
static int kfs_gc(lua_State *L);

static int kfs_new(lua_State *L)
{
    lua_Integer port = luaL_checkinteger(L, 1);
    lua_Integer conv = luaL_optinteger(L, 2, 0);
    kfs_client *c = (kfs_client *)lua_newuserdatauv(L, sizeof(kfs_client), 0);

    memset(c, 0, sizeof(*c));
    c->closed = 0;
    c->conv = (uint32_t)(conv & 0xFFFFFFFFu);
    pthread_mutex_init(&c->lock, NULL);

    c->net = net_open((int)port);
    if (c->net == NULL) {
        pthread_mutex_destroy(&c->lock);
        lua_pushnil(L);
        lua_pushstring(L, net_errmsg(NULL));
        return 2;
    }
    c->kcp = ikcp_create(c->conv, (void *)c);
    if (c->kcp == NULL) {
        net_close(c->net);
        c->net = NULL;
        pthread_mutex_destroy(&c->lock);
        lua_pushnil(L);
        lua_pushstring(L, "kfsclientlib: ikcp_create failed");
        return 2;
    }
    ikcp_setoutput(c->kcp, kfs_c_output);
    ikcp_nodelay(c->kcp, 1, 10, 2, 1);

    // 启动驱动线程
    c->drunning = 1;
    c->djoined = 0;
    if (pthread_create(&c->dthread, NULL, kfs_drive_thread, (void *)c) != 0) {
        c->drunning = 0;
        c->djoined = 1;
        ikcp_release(c->kcp);
        c->kcp = NULL;
        net_close(c->net);
        c->net = NULL;
        pthread_mutex_destroy(&c->lock);
        lua_pushnil(L);
        lua_pushstring(L, "kfsclientlib: thread create failed");
        return 2;
    }

    if (luaL_newmetatable(L, "kfsclientlib")) {
        static const luaL_Reg methods[] = {
            { "setpeer",       kfs_setpeer       },
            { "join",          kfs_join          },
            { "send_input",    kfs_send_input    },
            { "send_ready",    kfs_send_ready    },
            { "recv_broadcast",kfs_recv_broadcast},
            { "report_state",  kfs_report_state  },
            { "catch_up",      kfs_catch_up      },
            { "set_heartbeat", kfs_set_heartbeat },
            { "nodelay",       kfs_nodelay       },
            { "wndsize",       kfs_wndsize       },
            { "setmtu",        kfs_setmtu        },
            { "closed",        kfs_closed        },
            { "close",         kfs_close         },
            { "__gc",          kfs_gc            },
            { NULL, NULL }
        };
        luaL_setfuncs(L, methods, 0);
        lua_pushliteral(L, "__index");
        lua_pushvalue(L, -2);
        lua_rawset(L, -3);
        lua_pushliteral(L, "__metatable");
        lua_pushliteral(L, "kfsclientlib");
        lua_rawset(L, -3);
    }
    lua_setmetatable(L, -2);
    return 1;
}

// setpeer(ip, port) — 指定服务器地址
static int kfs_setpeer(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    const char *ip = luaL_checkstring(L, 2);
    lua_Integer port = luaL_checkinteger(L, 3);
    struct sockaddr_in peer;
    int ret;

    memset(&peer, 0, sizeof(peer));
    peer.sin_family = AF_INET;
    peer.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &peer.sin_addr) != 1)
        return luaL_error(L, "error kfs_client_lua: invalid ip '%s'", ip);

    pthread_mutex_lock(&c->lock);
    ret = (c->net && !c->closed) ? net_set_peer(c->net, &peer) : 0;
    pthread_mutex_unlock(&c->lock);
    lua_pushinteger(L, ret);
    return 1;
}

// join(room_id[, token]) — 发裸 UDP JOIN 到服务器并等待应答，登记进房间。
//   服务器从 JOIN 包源地址学到本客户端 ip:port，故无需显式绑定固定端口。
//   token：会话鉴权令牌（可选；0=不校验，须与服务器 new_room 配置一致）。
//   返回 1 成功；0 失败（第二返回值为错误描述）。
static int kfs_join(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    lua_Integer room_id = luaL_checkinteger(L, 2);
    lua_Integer token = luaL_optinteger(L, 3, 0);
    char hs[KFS_HS_LEN2];
    uint32_t m = KFS_HANDSHAKE_MAGIC;
    uint32_t rid = (uint32_t)(room_id & 0xFFFFFFFFu);
    uint32_t tok = (uint32_t)(token & 0xFFFFFFFFu);
    uint64_t deadline;
    int ret;

    memcpy(hs, &m, 4);
    memcpy(hs + 4, &rid, 4);
    memcpy(hs + 8, &c->conv, 4);
    memcpy(hs + 12, &tok, 4);

    pthread_mutex_lock(&c->lock);
    c->joined = 0;
    c->join_ok = 0;
    ret = (c->net && !c->closed) ? net_send(c->net, hs, KFS_HS_LEN2) : -1;
    pthread_mutex_unlock(&c->lock);
    if (ret < 0) {
        lua_pushboolean(L, 0);
        lua_pushstring(L, "no server peer, call setpeer first");
        return 2;
    }

    // 等待服务器应答（驱动线程异步更新 joined/join_ok），超时 5s
    deadline = kfs_now_ms() + 5000;
    while (kfs_now_ms() < deadline) {
        pthread_mutex_lock(&c->lock);
        if (c->joined) {
            int ok = c->join_ok;
            pthread_mutex_unlock(&c->lock);
            lua_pushboolean(L, ok);
            if (!ok)
                lua_pushstring(L, "server rejected join");
            return ok ? 1 : 2;
        }
        pthread_mutex_unlock(&c->lock);
        {
            struct timespec ts = {0, 5 * 1000 * 1000};   // 5ms 轮询
            nanosleep(&ts, NULL);
        }
    }
    lua_pushboolean(L, 0);
    lua_pushstring(L, "join timeout");
    return 2;
}

// send_input(frame_id, data) — 上报一帧输入；返回 0 成功
static int kfs_send_input(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    lua_Integer fid = luaL_checkinteger(L, 2);
    size_t len;
    const char *data = luaL_checklstring(L, 3, &len);
    char pkt[KFS_FRAME_MAX_TOTAL];
    int total, ret;

    if (len > (size_t)KFS_FRAME_MAX_DATA)
        return luaL_error(L, "error kfs_client_lua: input too large (%zu)", len);
    total = frame_encode2(pkt, (int)sizeof(pkt), KFS_CMD_INPUT,
                          (uint32_t)(fid & 0xFFFFFFFFu), data, (int)len);
    if (total < 0)
        return luaL_error(L, "error kfs_client_lua: frame encode failed");

    pthread_mutex_lock(&c->lock);
    if (c->kcp == NULL || c->closed) {
        pthread_mutex_unlock(&c->lock);
        return luaL_error(L, "error kfs_client_lua: client closed");
    }
    if (!c->joined || !c->join_ok) {
        pthread_mutex_unlock(&c->lock);
        return luaL_error(L, "error kfs_client_lua: not joined, call join() first");
    }
    ret = ikcp_send(c->kcp, pkt, total);
    pthread_mutex_unlock(&c->lock);
    lua_pushinteger(L, ret);
    return 1;
}

// send_ready() — 上报就绪：游戏数据加载完毕后调用。
//   房间未 start 前（准备阶段）服务器不推帧，靠心跳保活、靠 ready 计数
//   判定全员就绪；业务层据此调 room:start() 开始同步推帧。
static int kfs_send_ready(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    char pkt[KFS_FRAME_MAX_TOTAL];
    int total, ret;

    total = frame_encode2(pkt, (int)sizeof(pkt), KFS_CMD_READY, 0, NULL, 0);
    if (total < 0)
        return luaL_error(L, "error kfs_client_lua: frame encode failed");

    pthread_mutex_lock(&c->lock);
    if (c->kcp == NULL || c->closed) {
        pthread_mutex_unlock(&c->lock);
        return luaL_error(L, "error kfs_client_lua: client closed");
    }
    if (!c->joined || !c->join_ok) {
        pthread_mutex_unlock(&c->lock);
        return luaL_error(L, "error kfs_client_lua: not joined, call join() first");
    }
    ret = ikcp_send(c->kcp, pkt, total);
    pthread_mutex_unlock(&c->lock);
    lua_pushinteger(L, ret);
    return 1;
}

// recv_broadcast() → 收服务器广播帧；返回 frame_id, 广播负载；无则 nil
//   仅取出 CMD_BROADCAST 类型的帧，其他类型（如控制帧）丢弃。
static int kfs_recv_broadcast(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    int size, ret;
    char *pkt;
    uint8_t type;
    uint32_t fid;
    const char *data;
    int dlen;

    pthread_mutex_lock(&c->lock);
    if (c->kcp == NULL || c->closed) {
        pthread_mutex_unlock(&c->lock);
        return luaL_error(L, "error kfs_client_lua: client closed");
    }
    size = ikcp_peeksize(c->kcp);
    if (size < KFS_FRAME2_HEADER_SIZE) {
        pthread_mutex_unlock(&c->lock);
        return 0;
    }
    if (size > KFS_FRAME_MAX_TOTAL) {
        pthread_mutex_unlock(&c->lock);
        return luaL_error(L, "error kfs_client_lua: bad frame size %d", size);
    }
    pkt = (char *)malloc((size_t)size);
    if (pkt == NULL) {
        pthread_mutex_unlock(&c->lock);
        return luaL_error(L, "error kfs_client_lua: out of memory");
    }
    ret = ikcp_recv(c->kcp, pkt, size);
    pthread_mutex_unlock(&c->lock);

    if (ret < KFS_FRAME2_HEADER_SIZE) {
        free(pkt);
        return 0;
    }
    if (frame_decode2(pkt, ret, &type, &fid, &data, &dlen) != 0) {
        free(pkt);
        return luaL_error(L, "error kfs_client_lua: bad frame payload");
    }
    if (type == KFS_CMD_ALL_READY){
        // 处理全部准备完成

        free(pkt);
        return 0;
    }else if (type != KFS_CMD_BROADCAST) {
        free(pkt);                 // 非广播帧：丢弃
        return 0;
    }
    // 帧对账：广播 payload 末尾 4B 为服务器对该帧全员输入的 FNV-1a32。
    // 客户端重算 body 比对，一致为 true；不一致即判定该帧不同步（false）。
    if (dlen >= 4) {
        uint32_t h_sent, h_calc;
        memcpy(&h_sent, data + dlen - 4, 4);
        h_calc = frame_fnv1a(data, dlen - 4);
        lua_pushinteger(L, (lua_Integer)fid);
        lua_pushlstring(L, data, (size_t)dlen);
        lua_pushboolean(L, (h_calc == h_sent));
    } else {
        lua_pushinteger(L, (lua_Integer)fid);
        lua_pushlstring(L, data, (size_t)dlen);
        lua_pushboolean(L, 0);
    }
    free(pkt);
    return 3;
}

// report_state(frame_id, hash) — 上报某帧逻辑状态哈希（4B），供服务器对账
//   （服务器收集全员 state_hash 比对推进一致点 / 标记不同步）。
static int kfs_report_state(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    lua_Integer fid = luaL_checkinteger(L, 2);
    lua_Integer h   = luaL_checkinteger(L, 3);
    char pkt[KFS_FRAME_MAX_TOTAL];
    uint32_t hh = (uint32_t)(h & 0xFFFFFFFFu);
    int total, ret;

    total = frame_encode2(pkt, (int)sizeof(pkt), KFS_CMD_STATE,
                          (uint32_t)(fid & 0xFFFFFFFFu),
                          (const char *)&hh, 4);
    if (total < 0)
        return luaL_error(L, "error kfs_client_lua: frame encode failed");

    pthread_mutex_lock(&c->lock);
    if (c->kcp == NULL || c->closed) {
        pthread_mutex_unlock(&c->lock);
        return luaL_error(L, "error kfs_client_lua: client closed");
    }
    if (!c->joined || !c->join_ok) {
        pthread_mutex_unlock(&c->lock);
        return luaL_error(L, "error kfs_client_lua: not joined, call join() first");
    }
    ret = ikcp_send(c->kcp, pkt, total);
    pthread_mutex_unlock(&c->lock);
    lua_pushinteger(L, ret);
    return 1;
}

// catch_up() — 追帧：丢弃 KCP 接收缓冲里积压的旧广播，只保留最新一帧返回。
//   用于客户端严重滞后时快速跳到服务器当前帧（比逐帧 recv 更高效）。
//   返回 (frame_id, 负载, 对账ok)；无广播则 nil。
static int kfs_catch_up(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    char scratch[KFS_FRAME_MAX_TOTAL];
    int got = 0;
    uint32_t last_fid = 0;
    int last_len = 0;
    char *last_pkt = NULL;
    int last_ok = 0;

    pthread_mutex_lock(&c->lock);
    if (c->kcp == NULL || c->closed) {
        pthread_mutex_unlock(&c->lock);
        return luaL_error(L, "error kfs_client_lua: client closed");
    }
    while (1) {
        int size = ikcp_peeksize(c->kcp);
        int r;
        uint8_t type;
        uint32_t fid;
        const char *data;
        int dlen;
        if (size < KFS_FRAME2_HEADER_SIZE)
            break;
        if (size > KFS_FRAME_MAX_TOTAL) {
            ikcp_recv(c->kcp, scratch, (int)sizeof(scratch));   // 坏帧丢弃
            continue;
        }
        r = ikcp_recv(c->kcp, scratch, size);
        if (r < KFS_FRAME2_HEADER_SIZE)
            break;
        if (frame_decode2(scratch, r, &type, &fid, &data, &dlen) != 0)
            continue;
        if (type != KFS_CMD_BROADCAST)
            continue;                           // 非广播帧：丢弃
        free(last_pkt);                          // 丢弃更旧的广播，只留最新
        last_pkt = (char *)malloc((size_t)(dlen > 0 ? dlen : 1));
        if (last_pkt == NULL)
            break;
        if (dlen > 0)
            memcpy(last_pkt, data, (size_t)dlen);
        last_len = dlen;
        last_fid = fid;
        got = 1;
        last_ok = 0;
        if (dlen >= 4) {
            uint32_t h_sent, h_calc;
            memcpy(&h_sent, data + dlen - 4, 4);
            h_calc = frame_fnv1a(data, dlen - 4);
            last_ok = (h_calc == h_sent);
        }
    }
    pthread_mutex_unlock(&c->lock);

    if (!got || last_pkt == NULL)
        return 0;
    lua_pushinteger(L, (lua_Integer)last_fid);
    lua_pushlstring(L, last_pkt, (size_t)last_len);
    lua_pushboolean(L, last_ok);
    free(last_pkt);
    return 3;
}

// set_heartbeat(ms) — 设置自动心跳间隔（0=关闭；驱动线程按此定期补发 HEARTBEAT）
static int kfs_set_heartbeat(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    int ms = (int)luaL_optinteger(L, 2, 0);
    pthread_mutex_lock(&c->lock);
    c->heartbeat_ms = (ms < 0) ? 0 : ms;
    c->last_heartbeat_ms = kfs_now_ms();        // 从现在起计时
    pthread_mutex_unlock(&c->lock);
    return 0;
}

// nodelay / wndsize / setmtu — KCP 调优透传
static int kfs_nodelay(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    int nodelay = (int)luaL_checkinteger(L, 2);
    int interval = (int)luaL_checkinteger(L, 3);
    int resend   = (int)luaL_checkinteger(L, 4);
    int nc       = (int)luaL_checkinteger(L, 5);
    int ret;
    pthread_mutex_lock(&c->lock);
    ret = (c->kcp) ? ikcp_nodelay(c->kcp, nodelay, interval, resend, nc) : -1;
    pthread_mutex_unlock(&c->lock);
    lua_pushinteger(L, ret);
    return 1;
}

static int kfs_wndsize(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    int sndwnd = (int)luaL_checkinteger(L, 2);
    int rcvwnd = (int)luaL_checkinteger(L, 3);
    int ret;
    pthread_mutex_lock(&c->lock);
    ret = (c->kcp) ? ikcp_wndsize(c->kcp, sndwnd, rcvwnd) : -1;
    pthread_mutex_unlock(&c->lock);
    lua_pushinteger(L, ret);
    return 1;
}

static int kfs_setmtu(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    int mtu = (int)luaL_checkinteger(L, 2);
    int ret;
    pthread_mutex_lock(&c->lock);
    ret = (c->kcp) ? ikcp_setmtu(c->kcp, mtu) : -1;
    pthread_mutex_unlock(&c->lock);
    lua_pushinteger(L, ret);
    return 1;
}

static int kfs_closed(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    lua_pushboolean(L, c->closed);
    return 1;
}

// close() — 停驱动线程、关 socket、释放 KCP（幂等）
static void kfs_close_impl(kfs_client *c)
{
    // 置 closed，让驱动线程安全退出（驱动线程见 closed 后不再访问 net/kcp）
    pthread_mutex_lock(&c->lock);
    c->closed = 1;
    pthread_mutex_unlock(&c->lock);

    if (c->djoined == 0) {
        pthread_join(c->dthread, NULL);   // 先 join，确保驱动线程已退出
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
}

static int kfs_close(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    kfs_close_impl(c);
    return 0;
}

static int kfs_gc(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    kfs_close_impl(c);
    pthread_mutex_destroy(&c->lock);
    return 0;
}

//---------------------------------------------------------------------
// 模块入口
//---------------------------------------------------------------------
static const luaL_Reg kfs_client_module[] = {
    { "new", kfs_new },
    { NULL, NULL }
};

#if defined(__GNUC__)
#define KFS_API __attribute__((visibility("default")))
#else
#define KFS_API
#endif

KFS_API int luaopen_kfsclientlib(lua_State *L)
{
    luaL_newlib(L, kfs_client_module);
    return 1;
}

