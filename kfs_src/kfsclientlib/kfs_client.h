//=====================================================================
// kfs_client.h — 客户端库内部共享声明
//   供 client/kfs_client_core.c（驱动 + KCP output）与
//   client/kfs_client_lua.c（Lua 绑定）共用。
//   公共工具（kfs_now_ms）见 common/kfs_common.h。
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

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

// 符号可见性：Makefile 使用 -fvisibility=hidden，必须显式暴露 luaopen_*
#ifdef __GNUC__
#define KFS_API __attribute__((visibility("default")))
#else
#define KFS_API
#endif

//---------------------------------------------------------------------
// 客户端对象（单会话）
//---------------------------------------------------------------------
typedef struct kfs_client {
    kfs_net *net;               // 底层网络（net.c）
    ikcpcb  *kcp;               // 到服务器的 KCP 会话
    uint32_t conv;              // KCP conv（与服务器 add_player 一致）
    int      closed;            // 1=已关闭
    int      joined;            // 已收到服务器 JOIN 应答（无论成败）
    int      join_ok;           // JOIN 是否成功（1=已登记进房间）

    pthread_mutex_t lock;       // 保护 net/kcp/closed
    pthread_t   dthread;        // 后台驱动线程
    int         drunning;       // 驱动线程运行标志
    int         djoined;        // 驱动线程已 join

    int         heartbeat_ms;   // 自动心跳间隔（0=不启用）；驱动线程按此补发 HEARTBEAT
    uint64_t    last_heartbeat_ms;  // 上次发心跳的时间（单调时钟 ms）
} kfs_client;

//---------------------------------------------------------------------
// 跨模块函数声明（kfs_client_core.c 实现；kfs_client_lua.c 调用）
//---------------------------------------------------------------------
extern int kfs_c_output(const char *buf, int len, ikcpcb *kcp, void *user);
extern void *kfs_drive_thread(void *arg);

#endif /* KFS_CLIENT_H */
