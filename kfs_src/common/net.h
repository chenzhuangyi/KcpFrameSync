//=====================================================================
//
// net.h — KCP 帧同步库的网络连接层（纯 C，不依赖 Lua）
//
// 职责：管理一个 UDP socket 的完整生命周期 —— 创建/绑定端口、
//       启动接收线程、原始 UDP 包收发队列、对端地址学习与发送。
//
// 设计：
//   - 本模块不依赖 Lua，也不感知 KCP/帧同步。
//   - 接收线程只把收到的原始 UDP 包放入有锁队列；
//     调用方（Lua 接入层）通过 net_recv() 轮询取出。
//   - 发送通过 net_send() 发往已学到的对端地址。
//   - 对端地址（点对点）由调用方在取出首包后调用
//     net_set_peer() 写入，或由本模块在首个包到达时自动学习。
//
//=====================================================================
#ifndef KFS_NET_H
#define KFS_NET_H

#include <stddef.h>
#include <netinet/in.h>

#ifdef __cplusplus
extern "C" {
#endif

// UDP 单包缓冲上限（IPv4 最大 payload）
#define KFS_NET_MAX_PKT  65535

// 接收线程原始包队列容量
#define KFS_NET_QUEUE_CAP 1024

// 不透明句柄
typedef struct kfs_net kfs_net;

// 创建并绑定 UDP socket，启动接收线程。
//   port : 本机 UDP 端口（0 表示由系统分配）
//   成功返回句柄；失败返回 NULL（可通过 net_errmsg 查看原因）
kfs_net *net_open(int port);

// 取一个原始 UDP 包。返回实际拷贝到 out 的字节数（>0 有包）；
// 无包返回 0。
//   out / out_cap : 接收缓冲与容量
//   from          : （可选）对端地址，可为 NULL
int net_recv(kfs_net *n, char *out, int out_cap, struct sockaddr_in *from);

// 向已学到的对端发送数据。返回实际发送字节数；未学到对端返回 -1。
int net_send(kfs_net *n, const char *buf, int len);

// 向指定对端发送数据（多人广播：逐玩家调本接口）。
// 返回实际发送字节数；失败返回 -1。
int net_send_to(kfs_net *n, const struct sockaddr_in *peer,
                const char *buf, int len);

// 显式设置对端地址（点对点）。返回 1 成功，0 失败。
int net_set_peer(kfs_net *n, const struct sockaddr_in *peer);

// 查询是否已学到对端地址。1 = 已设置，0 = 未设置。
int net_peer_known(kfs_net *n);

// 关闭 socket、停止接收线程并释放句柄（幂等，可对 NULL 调用）。
void net_close(kfs_net *n);

// 最近一次失败的描述（供报错用）。
const char *net_errmsg(kfs_net *n);

#ifdef __cplusplus
}
#endif

#endif /* KFS_NET_H */
