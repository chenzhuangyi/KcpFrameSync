//=====================================================================
// kfs_lua.c — 服务器库 Lua 绑定层
//   fs.new / conn 方法 / 点对点房间方法 / 服务器多玩家房间方法 / 模块入口
//   底层逻辑见 kfs_core.c；结构定义见 kfs_server.h。
//=====================================================================
#include "kfs_server.h"

/* 可选：registry 钉住唯一 conn，防 GC 误关 */
static int g_conn_ref = LUA_NOREF;

//=====================================================================
// 内部小工具
//=====================================================================
static kfs_conn_t *check_conn(lua_State *L, int idx)
{
    return (kfs_conn_t *)luaL_checkudata(L, idx, "kfsserverlib");
}

static kfs_room_t *check_room(lua_State *L, int idx)
{
    return (kfs_room_t *)luaL_checkudata(L, idx, "kfsserverlib.room");
}

static kfs_srvroom_t *check_srvroom(lua_State *L, int idx)
{
    return (kfs_srvroom_t *)luaL_checkudata(L, idx, "kfsserverlib.srvroom");
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
static int kfs_srv_init_count(lua_State *L);
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

//=====================================================================
// 工厂：fs.new(port)  → 返回连接实例（只绑定端口，未创建房间）
//  绑定端口成功后，自动启动驱动线程（见 netref_init）。
//=====================================================================
static int kfs_init(lua_State *L)
{
    // 已初始化：把已钉住的 conn 压回栈复用（幂等，仍是单例）
    if (g_conn_ref != LUA_NOREF) {
        lua_rawgeti(L, LUA_REGISTRYINDEX, g_conn_ref);
        return 1;
    }

    lua_Integer port = luaL_checkinteger(L, 1);
    lua_Integer fps  = luaL_optinteger(L, 2, 20);   // frame_rate，默认 20
    if (fps < 1) fps = 1;
    if (fps > 60) fps = 60;
    uint32_t interval = (uint32_t)(1000 / fps);
    if (interval < 1) interval = 1;

    kfs_netref *r = netref_init(port, interval);  // 单例，端口占用/重复返回 NULL
    if (r == NULL) {
        lua_pushnil(L);
        lua_pushstring(L, net_errmsg(NULL));
        return 2;
    }

    // 1) 先建元表并挂到 self 上，确保 self 回到栈顶后再 ref
    kfs_conn_t *self = (kfs_conn_t *)lua_newuserdatauv(L, sizeof(kfs_conn_t), 0);
    self->r = r;   // 走"钉住 + GC 哑火"路线：不 addref，共享 netref_init 的 refs=1

    if (luaL_newmetatable(L, "kfsserverlib")) {
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
        lua_pushliteral(L, "kfsserverlib");
        lua_rawset(L, -3);
    }
    lua_setmetatable(L, -2);   // 此刻栈：[..., self(带元表)]，self 是栈顶

    // 2) self 在栈顶，ref 弹的才是 self —— 钉住的是 conn，不是元表
    g_conn_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    lua_rawgeti(L, LUA_REGISTRYINDEX, g_conn_ref);  // 推回 self 作为返回值
    return 1;
}

// kfs_shutdown()：显式关端口（主线程调用）
static int l_kfs_shutdown(lua_State *L) {
    if (g_conn_ref == LUA_NOREF) return 0;
    lua_rawgeti(L, LUA_REGISTRYINDEX, g_conn_ref);
    kfs_conn_t *c = (kfs_conn_t *)lua_touserdata(L, -1);  // 修正：标准 API
    if (c && c->r)
        netref_close(c->r);          // 主线程：stop + join + 清 g_netref
    luaL_unref(L, LUA_REGISTRYINDEX, g_conn_ref);
    g_conn_ref = LUA_NOREF;
    lua_pop(L, 1);
    return 0;
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
        return luaL_error(L, "error kfs_lua: connection gone");

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
        return luaL_error(L, "error kfs_lua: ikcp_create failed");
    }

    // 持锁注册路由 + 设置输出回调/默认参数
    pthread_mutex_lock(&r->lock);
    if (r->closed) {
        pthread_mutex_unlock(&r->lock);
        ikcp_release(room->kcp);
        room->kcp = NULL;
        netref_release(room->r);
        room->r = NULL;
        return luaL_error(L, "error kfs_lua: connection closed");
    }
    if (netref_route_add(r, (IUINT32)conv, room->kcp) != 0) {
        pthread_mutex_unlock(&r->lock);
        ikcp_release(room->kcp);
        room->kcp = NULL;
        netref_release(room->r);
        room->r = NULL;
        return luaL_error(L, "error kfs_lua: conv %d already in use",
                          (int)conv);
    }
    ikcp_setoutput(room->kcp, kfs_output);
    ikcp_nodelay(room->kcp, 1, 10, 2, 1);   // 默认快速模式，可被 Lua 覆盖
    pthread_mutex_unlock(&r->lock);

    // metatable（锁外）
    if (luaL_newmetatable(L, "kfsserverlib.room")) {
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
        lua_pushliteral(L, "kfsserverlib.room");
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
        return luaL_error(L, "error kfs_lua: connection gone");
    pthread_mutex_lock(&r->lock);
    if (r->closed) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: connection closed");
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
        return luaL_error(L, "error kfs_lua: invalid ip '%s'", ip);

    if (r == NULL)
        return luaL_error(L, "error kfs_lua: connection gone");
    pthread_mutex_lock(&r->lock);
    if (r->closed || r->net == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: connection closed");
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

// __gc：钉住路线下空实现——端口生死只由 netref_close（shutdown / conn:close）决定
static int kfs_conn_gc(lua_State *L)
{
    (void)L;
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
        return luaL_error(L, "error kfs_lua: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: room closed");
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
        return luaL_error(L, "error kfs_lua: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: room closed");
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
        return luaL_error(L, "error kfs_lua: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: room closed");
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
        return luaL_error(L, "error kfs_lua: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: room closed");
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
        return luaL_error(L, "error kfs_lua: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: room closed");
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
        return luaL_error(L, "error kfs_lua: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: room closed");
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
        return luaL_error(L, "error kfs_lua: send data too large (%zu)", len);
    if (r == NULL)
        return luaL_error(L, "error kfs_lua: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: room closed");
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
        return luaL_error(L, "error kfs_lua: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: room closed");
    }
    size = ikcp_peeksize(room->kcp);
    if (size < 0) {
        pthread_mutex_unlock(&r->lock);
        return 0;
    }
    buf = (char *)malloc((size_t)size);
    if (buf == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: out of memory");
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
        return luaL_error(L, "error kfs_lua: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: room closed");
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
        return luaL_error(L, "error kfs_lua: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: room closed");
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
        return luaL_error(L, "error kfs_lua: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: room closed");
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
        return luaL_error(L, "error kfs_lua: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: room closed");
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
        return luaL_error(L, "error kfs_lua: frame too large (%zu)", len);
    if (len > (size_t)0x7FFF - KFS_FRAME_HEADER_SIZE)
        return luaL_error(L, "error kfs_lua: frame exceeds kcp message limit");
    total = frame_encode(pkt, (int)sizeof(pkt),
                         (uint32_t)(fid & 0xFFFFFFFFu), data, (int)len);
    if (total < 0)
        return luaL_error(L, "error kfs_lua: frame encode failed");

    if (r == NULL)
        return luaL_error(L, "error kfs_lua: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: room closed");
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
        return luaL_error(L, "error kfs_lua: room gone");
    pthread_mutex_lock(&r->lock);
    if (room->kcp == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: room closed");
    }

    size = ikcp_peeksize(room->kcp);
    if (size < KFS_FRAME_HEADER_SIZE) {
        pthread_mutex_unlock(&r->lock);
        return 0;
    }
    if (size > KFS_FRAME_MAX_TOTAL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: bad frame size %d", size);
    }
    pkt = (char *)malloc((size_t)size);
    if (pkt == NULL) {
        pthread_mutex_unlock(&r->lock);
        return luaL_error(L, "error kfs_lua: out of memory");
    }
    ret = ikcp_recv(room->kcp, pkt, size);
    pthread_mutex_unlock(&r->lock);

    if (ret < KFS_FRAME_HEADER_SIZE) { free(pkt); return 0; }
    if (frame_decode(pkt, ret, &fid, &data, &dlen) != 0) {
        free(pkt);
        return luaL_error(L, "error kfs_lua: bad frame payload");
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
        return luaL_error(L, "error kfs_lua: connection gone");

    // 收集初始玩家列表（锁外遍历玩家表：key=玩家 id(conv)）
    if (has_players) {
        lua_pushnil(L);
        while (lua_next(L, 7) != 0) {
            // 设定的最大玩家数：1..KFS_ROOM_MAX_PLAYER-1（数组上限之外拒绝）
            if (init_count >= KFS_ROOM_MAX_PLAYER)
                return luaL_error(L, "error kfs_lua: init_count error! init_count>%d", (int)KFS_ROOM_MAX_PLAYER);
        
            
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
        return luaL_error(L, "error kfs_lua: new_room failed (rc=%d)", rc);
    }

    // 诊断：打印初始玩家登记（临时调试，确认后删除）
    LOG_DEBUGF("new_room room=%u init_count=%d", (unsigned)srv->room_id, init_count);
    for (i = 0; i < init_count; i++)
    LOG_DEBUGF("初始玩家 conv=%u", (unsigned)init_convs[i]);

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

    if (luaL_newmetatable(L, "kfsserverlib.srvroom")) {
        static const luaL_Reg methods[] = {
            { "add_player",    kfs_srv_add_player    },
            { "remove_player", kfs_srv_remove_player },
            { "set_player_token", kfs_srv_set_player_token },
            { "get_id",        kfs_srv_get_id        },
            { "get_count",     kfs_srv_get_count     },
            { "start",         kfs_srv_start         },
            { "init_count",    kfs_srv_init_count    },
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
        lua_pushliteral(L, "kfsserverlib.srvroom");
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
        return luaL_error(L, "error kfs_lua: invalid ip '%s'", ip);
    if (r == NULL || srv->closed)
        return luaL_error(L, "error kfs_lua: room gone");
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
        return luaL_error(L, "error kfs_lua: room gone");
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
        return luaL_error(L, "error kfs_lua: room gone");
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
        return luaL_error(L, "error kfs_lua: room gone");
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
static int kfs_srv_init_count(lua_State *L)
{
    kfs_srvroom_t *srv = check_srvroom(L, 1);
    kfs_netref *r = srv->r;
    int n = 0, j;

    if (r && !srv->closed) {
        pthread_mutex_lock(&r->lock);
        {
            kfs_room *room = kfs_room_find(r, srv->room_id);
            if (room)
                n = room->init_count;
        }
        pthread_mutex_unlock(&r->lock);
    }
    lua_pushinteger(L, n);
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

// 模块入口
//=====================================================================
static const luaL_Reg kfs_module[] = {
    { "init",    kfs_init },
    { "shutdown",    l_kfs_shutdown },
    { NULL, NULL }
};

// 必须对 Lua 可见：Makefile 用 -fvisibility=hidden，若不显式导出，
// Lua 用 dlsym 查找 luaopen_kfsserverlib 会失败（undefined symbol）。
#if defined(__GNUC__)
#define KFS_API __attribute__((visibility("default")))
#else
#define KFS_API
#endif

KFS_API int luaopen_kfsserverlib(lua_State *L)
{
    luaL_newlib(L, kfs_module);
    return 1;
}
