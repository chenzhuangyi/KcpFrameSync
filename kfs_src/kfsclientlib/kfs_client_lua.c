//=====================================================================
// kfs_client_lua.c — 客户端库 Lua 绑定层（瘦绑定）
//
//   本层只做三件事：
//     1. userdata 持有 kfs_client* 句柄（指针，非内联结构）；
//     2. 参数检查与 Lua 值 <-> C 值映射；
//     3. 把 ABI 状态码翻译回旧版 Lua 错误文案，保证兼容。
//   驱动线程、解帧、对账、事件队列等全部逻辑在 kfs_client_core.c，
//   经 kfs_client_api.h 调用 —— Unity/Go/Rust 绑定接的是同一 ABI。
//
//   对外方法（与旧版一致）：
//     kfs.new(port[, conv]) → client | nil, err
//     c:setpeer(ip, port)          c:join(room_id[, token])
//     c:send_input(fid, data)      c:send_ready()
//     c:recv_broadcast()           c:report_state(fid, hash)
//     c:catch_up()                 c:set_heartbeat(ms)
//     c:nodelay(..) c:wndsize(..) c:setmtu(..)
//     c:closed()  c:close()
//   新增（ABI 能力透传，旧脚本不感知）：
//     c:join_start(room_id[, token])  c:join_check()
//     c:frame_interval_ms()  c:room_seed()  c:server_time_offset_ms()
//     c:start_fid()  c:pending_events()  kfs.version()
//=====================================================================
#include "kfs_client_api.h"

#include <string.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

//---------------------------------------------------------------------
// userdata：一个指针槽。close() 后置 NULL，__gc 幂等。
//---------------------------------------------------------------------
typedef struct {
    kfs_client *c;
} kfs_udata;

static kfs_client *check_client(lua_State *L, int idx)
{
    kfs_udata *ud = (kfs_udata *)luaL_checkudata(L, idx, "kfsclientlib");
    if (ud->c == NULL)
        luaL_error(L, "error kfs_client_lua: client closed");
    return ud->c;
}

// ABI 错误码 → 旧版运行时错误文案（send_* 路径）
static void client_send_error(lua_State *L, int status)
{
    switch (status) {
    case KFS_ERR_CLOSED:
        luaL_error(L, "error kfs_client_lua: client closed");
        break;
    case KFS_ERR_NOT_JOINED:
        luaL_error(L, "error kfs_client_lua: not joined, call join() first");
        break;
    default:
        luaL_error(L, "error kfs_client_lua: send failed (%d)", status);
        break;
    }
}

//---------------------------------------------------------------------
// 方法前向声明（metatable 在工厂函数中引用，须先声明后使用）
//---------------------------------------------------------------------
static int kfs_setpeer(lua_State *L);
static int kfs_join(lua_State *L);
static int kfs_join_start(lua_State *L);
static int kfs_join_check(lua_State *L);
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
static int kfs_frame_interval(lua_State *L);
static int kfs_room_seed(lua_State *L);
static int kfs_time_offset(lua_State *L);
static int kfs_start_fid(lua_State *L);
static int kfs_is_started(lua_State *L);
static int kfs_pending_events(lua_State *L);

static int kfs_new(lua_State *L)
{
    lua_Integer port = luaL_checkinteger(L, 1);
    lua_Integer conv = luaL_optinteger(L, 2, 0);
    const char *err = NULL;
    kfs_client *c;
    kfs_udata *ud;

    c = kfs_client_create((uint16_t)port, (uint32_t)(conv & 0xFFFFFFFFu), &err);
    if (c == NULL) {
        lua_pushnil(L);
        lua_pushstring(L, err ? err : "kfsclientlib: create failed");
        return 2;
    }

    ud = (kfs_udata *)lua_newuserdatauv(L, sizeof(kfs_udata), 0);
    ud->c = c;

    if (luaL_newmetatable(L, "kfsclientlib")) {
        static const luaL_Reg methods[] = {
            { "setpeer",        kfs_setpeer        },
            { "join",           kfs_join           },
            { "join_start",     kfs_join_start     },
            { "join_check",     kfs_join_check     },
            { "send_input",     kfs_send_input     },
            { "send_ready",     kfs_send_ready     },
            { "recv_broadcast", kfs_recv_broadcast },
            { "report_state",   kfs_report_state   },
            { "catch_up",       kfs_catch_up       },
            { "set_heartbeat",  kfs_set_heartbeat  },
            { "nodelay",        kfs_nodelay        },
            { "wndsize",        kfs_wndsize        },
            { "setmtu",         kfs_setmtu         },
            { "closed",         kfs_closed         },
            { "close",          kfs_close          },
            { "frame_interval_ms",  kfs_frame_interval },
            { "room_seed",      kfs_room_seed      },
            { "server_time_offset_ms", kfs_time_offset },
            { "start_fid",      kfs_start_fid      },
            { "is_started",     kfs_is_started     },
            { "pending_events", kfs_pending_events },
            { "__gc",           kfs_gc             },
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

// setpeer(ip, port) — 指定服务器地址；返回 1 成功 / 0 失败（旧语义）
static int kfs_setpeer(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    const char *ip = luaL_checkstring(L, 2);
    lua_Integer port = luaL_checkinteger(L, 3);
    int ret = kfs_client_set_peer(c, ip, (uint16_t)port);
    if (ret == KFS_ERR_PARAM)
        return luaL_error(L, "error kfs_client_lua: invalid ip '%s'", ip);
    lua_pushinteger(L, (ret == KFS_OK) ? 1 : 0);
    return 1;
}

// join(room_id[, token]) — 发裸 UDP JOIN 并等待应答（5s 超时，阻塞式）。
//   返回 1 成功；0 失败（第二返回值为错误描述，文案与旧版一致）。
static int kfs_join(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    lua_Integer room_id = luaL_checkinteger(L, 2);
    lua_Integer token = luaL_optinteger(L, 3, 0);
    int status = kfs_client_join(c, (uint32_t)(room_id & 0xFFFFFFFFu),
                                 (uint32_t)(token & 0xFFFFFFFFu));

    if (status == KFS_OK) {
        lua_pushboolean(L, 1);
        return 1;
    }
    lua_pushboolean(L, 0);
    switch (status) {
    case KFS_ERR_NO_PEER:
        lua_pushstring(L, "no server peer, call setpeer first");
        break;
    case KFS_ERR_TIMEOUT:
        lua_pushstring(L, "join timeout");
        break;
    default:
        lua_pushstring(L, "server rejected join");
        break;
    }
    return 2;
}

// join_start(room_id[, token]) — 非阻塞发起；返回 1 已发出 / 0 失败(原因)
static int kfs_join_start(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    lua_Integer room_id = luaL_checkinteger(L, 2);
    lua_Integer token = luaL_optinteger(L, 3, 0);
    int status = kfs_client_join_start(c, (uint32_t)(room_id & 0xFFFFFFFFu),
                                       (uint32_t)(token & 0xFFFFFFFFu));
    lua_pushinteger(L, (status == KFS_OK) ? 1 : 0);
    if (status != KFS_OK)
        lua_pushstring(L, kfs_client_last_error(c));
    return (status == KFS_OK) ? 1 : 2;
}

// join_check() — 查询异步 join 状态："pending"|"ok"|"rejected"|"timeout"
static int kfs_join_check(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    int status = kfs_client_join_check(c);
    switch (status) {
    case KFS_OK:           lua_pushliteral(L, "ok");       break;
    case KFS_ERR_PENDING:  lua_pushliteral(L, "pending");  break;
    case KFS_ERR_TIMEOUT:  lua_pushliteral(L, "timeout");  break;
    default:               lua_pushliteral(L, "rejected"); break;
    }
    return 1;
}

// send_input(frame_id, data) — 上报一帧输入；返回 0 成功
static int kfs_send_input(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    lua_Integer fid = luaL_checkinteger(L, 2);
    size_t len;
    const char *data = luaL_checklstring(L, 3, &len);
    int status;

    if (len > (size_t)0xFFFF)
        return luaL_error(L, "error kfs_client_lua: input too large (%zu)", len);
    status = kfs_client_send_input(c, (uint32_t)(fid & 0xFFFFFFFFu),
                                   data, (int)len);
    if (status < 0)
        client_send_error(L, status);
    lua_pushinteger(L, status);
    return 1;
}

// send_ready() — 上报就绪（游戏数据加载完毕）；返回 0 成功
static int kfs_send_ready(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    int status = kfs_client_send_ready(c);
    if (status < 0)
        client_send_error(L, status);
    lua_pushinteger(L, status);
    return 1;
}

// 填充 (fid, payload, 对账ok) 三返回值
static int push_frame_event(lua_State *L, const kfs_client_event *ev)
{
    lua_pushinteger(L, (lua_Integer)ev->fid);
    if (ev->data != NULL && ev->len > 0)
        lua_pushlstring(L, (const char *)ev->data, (size_t)ev->len);
    else
        lua_pushliteral(L, "");
    lua_pushboolean(L, ev->consistent);
    return 3;
}

// recv_broadcast() → fid, 广播负载, 对账ok；无则 nil
//   仅消费 KFS_EVENT_FRAME；ALL_READY 等其他事件在 ABI 层自动记账分发。
static int kfs_recv_broadcast(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    kfs_client_event ev;
    int ret;

    memset(&ev, 0, sizeof(ev));
    ret = kfs_client_poll_frame(c, &ev);
    if (ret < 0)
        return luaL_error(L, "error kfs_client_lua: client closed");
    if (ret == 0)
        return 0;
    ret = push_frame_event(L, &ev);
    kfs_client_event_release(&ev);
    return ret;
}

// report_state(frame_id, hash) — 上报某帧逻辑状态哈希（4B），供服务器对账
static int kfs_report_state(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    lua_Integer fid = luaL_checkinteger(L, 2);
    lua_Integer h   = luaL_checkinteger(L, 3);
    int status = kfs_client_report_state(c, (uint32_t)(fid & 0xFFFFFFFFu),
                                        (uint32_t)(h & 0xFFFFFFFFu));
    if (status < 0)
        client_send_error(L, status);
    lua_pushinteger(L, status);
    return 1;
}

// catch_up() — 追帧：丢弃积压旧广播，只返回最新一帧。
//   返回 (frame_id, 负载, 对账ok)；无广播则 nil。
static int kfs_catch_up(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    kfs_client_event ev;
    int ret;

    memset(&ev, 0, sizeof(ev));
    ret = kfs_client_catch_up(c, &ev);
    if (ret < 0)
        return luaL_error(L, "error kfs_client_lua: client closed");
    if (ret == 0)
        return 0;
    ret = push_frame_event(L, &ev);
    kfs_client_event_release(&ev);
    return ret;
}

// set_heartbeat(ms) — 设置自动心跳间隔（0=关闭）
static int kfs_set_heartbeat(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    int ms = (int)luaL_optinteger(L, 2, 0);
    kfs_client_set_heartbeat(c, ms);
    return 0;
}

// nodelay / wndsize / setmtu — KCP 调优透传
static int kfs_nodelay(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    int ret = kfs_client_nodelay(c,
        (int)luaL_checkinteger(L, 2), (int)luaL_checkinteger(L, 3),
        (int)luaL_checkinteger(L, 4), (int)luaL_checkinteger(L, 5));
    lua_pushinteger(L, ret);
    return 1;
}

static int kfs_wndsize(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    int ret = kfs_client_wndsize(c,
        (int)luaL_checkinteger(L, 2), (int)luaL_checkinteger(L, 3));
    lua_pushinteger(L, ret);
    return 1;
}

static int kfs_setmtu(lua_State *L)
{
    kfs_client *c = check_client(L, 1);
    int ret = kfs_client_setmtu(c, (int)luaL_checkinteger(L, 2));
    lua_pushinteger(L, ret);
    return 1;
}

// 会话锚点查询（join 成功后由驱动线程自动锚定）
static int kfs_frame_interval(lua_State *L)
{
    lua_pushinteger(L, (lua_Integer)kfs_client_frame_interval_ms(check_client(L, 1)));
    return 1;
}

static int kfs_room_seed(lua_State *L)
{
    lua_pushinteger(L, (lua_Integer)kfs_client_room_seed(check_client(L, 1)));
    return 1;
}

static int kfs_time_offset(lua_State *L)
{
    lua_pushinteger(L, (lua_Integer)kfs_client_server_time_offset_ms(check_client(L, 1)));
    return 1;
}

static int kfs_start_fid(lua_State *L)
{
    lua_pushinteger(L, (lua_Integer)kfs_client_start_fid(check_client(L, 1)));
    return 1;
}

static int kfs_is_started(lua_State *L)
{
    lua_pushboolean(L, kfs_client_is_started(check_client(L, 1)));
    return 1;
}

static int kfs_pending_events(lua_State *L)
{
    lua_pushinteger(L, (lua_Integer)kfs_client_pending_events(check_client(L, 1)));
    return 1;
}

static int kfs_closed(lua_State *L)
{
    kfs_udata *ud = (kfs_udata *)luaL_checkudata(L, 1, "kfsclientlib");
    lua_pushboolean(L, (ud->c == NULL || kfs_client_is_closed(ud->c)) ? 1 : 0);
    return 1;
}

// close() — 停驱动线程、关 socket、释放会话（幂等；置空防 __gc 二次销毁）
static int kfs_close(lua_State *L)
{
    kfs_udata *ud = (kfs_udata *)luaL_checkudata(L, 1, "kfsclientlib");
    if (ud->c != NULL) {
        kfs_client_destroy(ud->c);
        ud->c = NULL;
    }
    return 0;
}

static int kfs_gc(lua_State *L)
{
    return kfs_close(L);
}

//---------------------------------------------------------------------
// 模块入口
//---------------------------------------------------------------------
static int kfs_version(lua_State *L)
{
    lua_pushstring(L, kfs_client_version());
    return 1;
}

static const luaL_Reg kfs_client_module[] = {
    { "new",     kfs_new     },
    { "version", kfs_version },
    { NULL, NULL }
};

#if defined(__GNUC__)
#define KFS_LUA_API __attribute__((visibility("default")))
#else
#define KFS_LUA_API
#endif

KFS_LUA_API int luaopen_kfsclientlib(lua_State *L)
{
    luaL_newlib(L, kfs_client_module);
    return 1;
}
