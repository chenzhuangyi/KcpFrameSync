//=====================================================================
//
// frame.h — KCP 帧同步库的帧同步层（纯 C，不依赖 Lua / KCP / 网络）
//
// 职责：在 KCP 可靠字节流之上定义"帧"的封装协议，并负责编解码。
//
// 帧信封格式（host order）：
//   总包 = [ frame_id(4B, uint32) ][ data_len(2B, uint16) ][ data... ]
//   - 一帧即一条 KCP 消息（依赖 KCP 非 stream 模式保持消息边界）。
//   - data_len 为负载字节数，不含 6 字节帧头。
//
// 本模块不管理 socket、不调用 KCP；只做纯内存的帧打包/解包与校验，
// 供上层（Lua 接入层）使用。帧号由调用方（应用层/主机）统一分配。
//
//=====================================================================
#ifndef KFS_FRAME_H
#define KFS_FRAME_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif


//---------------------------------------------------------------------
// 常量
//---------------------------------------------------------------------
// 最大房间数（预留）
#define KFS_ROOM_MAX 256

// 房间最大玩家数
#define KFS_ROOM_MAX_PLAYER 32
// 单个客户端单帧输入上限（协议契约，全链路统一：
//   客户端发送侧主动拒绝 + 服务器收侧防御丢弃，单一事实源）
#define KFS_FRAME_INPUT_MAX  32


// 旧帧头长度（frame_id 4B + data_len 2B），向后兼容点对点测试
#define KFS_FRAME_HEADER_SIZE 6

// 新帧头长度（type 1B + frame_id 4B + data_len 2B）
#define KFS_FRAME2_HEADER_SIZE 7

// 单帧负载数据最大长度（受 2B 长度字段限制）
#define KFS_FRAME_MAX_DATA  0xFFFF

// 单帧总长物理上限（帧头 + 最大负载；两种帧头取较大者）
#define KFS_FRAME_MAX_TOTAL (KFS_FRAME2_HEADER_SIZE + KFS_FRAME_MAX_DATA)

// 超前输入窗口：玩家 KCP 里可能同时缓存未来多帧输入，用环形缓冲暂存，
// 容量须 ≥ 输入提前量窗口 K（+ 余量），供服务器按帧号取用。
#define KFS_PLAYER_BUF 8




//---------------------------------------------------------------------
// 裸 UDP 握手协议（KCP 会话建立前，客户端 → 服务器学习地址并登记）
//   JOIN 请求（带会话 token，鉴权）：
//     [ magic 4B ][ room_id 4B ][ conv 4B ][ token 4B ]   (16 字节)
//     兼容旧格式：无 token（12 字节）按 token=0 处理。
//   JOIN 应答：[ magic 4B ][ result 1B ][frame_interval_ms 4B][room_seed 4B][server_time_ms 8B]    (21 字节)
//     result: 0=成功  1=房间不存在  2=房间满  3=conv冲突
//             4=其他  5=token 错误
//   服务器据此从请求包源地址学到客户端的真实 ip:port 并登记进房间，
//   客户端因此不必为对齐端口而显式绑定固定端口。
//---------------------------------------------------------------------
#define KFS_HANDSHAKE_MAGIC 0x4B464346u   // "KFCF"，与 KCP 内部 cmd 值不冲突
#define KFS_HS_LEN          12            // 旧 JOIN 请求包长度（无 token）
#define KFS_HS_LEN2         16            // 新 JOIN 请求包长度（带 token）

//---------------------------------------------------------------------
// 帧消息类型（协议在 KCP 可靠字节流之上，单条 KCP 消息承载一帧）
//---------------------------------------------------------------------
typedef enum {
    KFS_CMD_INPUT      = 1,   // 客户端 → 服务器：上报一帧输入
    KFS_CMD_BROADCAST  = 2,   // 服务器 → 客户端：广播整帧（含全员输入）
    KFS_CMD_JOIN       = 3,   // 客户端 → 服务器：请求加入房间(KFS_HANDSHAKE_MAGIC)
    KFS_CMD_LEAVE      = 4,   // 客户端 → 服务器：离开房间
    KFS_CMD_HEARTBEAT  = 5,   // 心跳（保活 / 掉线判定）
    KFS_CMD_STATE      = 6,   // 客户端 → 服务器：上报某帧逻辑状态哈希（对账）
    KFS_CMD_READY      = 7,   // 客户端 → 服务器：游戏数据加载完毕，上报就绪
    KFS_CMD_READY_ACK  = 8,   // 服务器 → 客户端：加载和连接就绪回复（1）
    KFS_CMD_ALL_READY  = 9,   // 服务器 → 客户端：全部客户端加载和连接就绪回复（[server_time_ms 8B]）
    KFS_CMD_REJOIN_SYNC =10,
    KFS_CMD_PLAYER_REJOIN =11,
} kfs_cmd;
/* JOIN_RES payload结构：
 * uint32 frame_interval_ms
 * uint32 room_seed
 * uint64 server_time_ms
 * uint32 sync_fid        // 新增：最近一致锚点帧
 * uint32 collecting_fid   // 新增：当前房间正在收集的帧
 */



//---------------------------------------------------------------------
// 打包一帧。成功返回总字节数（>= KFS_FRAME_HEADER_SIZE）；失败返回 -1。
//   out / out_cap : 输出缓冲与容量
//   fid           : 帧号（应用层分配）
//   data / len    : 帧负载
//---------------------------------------------------------------------
int frame_encode(char *out, int out_cap, uint32_t fid,
                 const char *data, int len);

//---------------------------------------------------------------------
// 解包一帧。成功返回 0，并把帧号与负载指针/长度写出；失败返回 -1。
//   pkt / pktlen  : 收到的整帧数据（应 >= KFS_FRAME_HEADER_SIZE）
//   fid           : 输出帧号
//   data / dlen   : 输出负载指针与长度（指向 pkt 内部，不可跨调用保留）
//---------------------------------------------------------------------
int frame_decode(const char *pkt, int pktlen,
                 uint32_t *fid, const char **data, int *dlen);

//---------------------------------------------------------------------
// 带消息类型的帧打包（协议：KFS_FRAME2_HEADER_SIZE 字节头 + 负载）
//   布局：[ type 1B ][ frame_id 4B ][ data_len 2B ][ data... ]
//   成功返回总字节数（>= KFS_FRAME2_HEADER_SIZE）；失败返回 -1。
//---------------------------------------------------------------------
int frame_encode2(char *out, int out_cap, uint8_t type, uint32_t fid,
                  const char *data, int len);

//---------------------------------------------------------------------
// 带消息类型的帧解包。成功返回 0，写出 type / fid / data / dlen。
//   data / dlen 指向 pkt 内部，不可跨调用保留。
//---------------------------------------------------------------------
int frame_decode2(const char *pkt, int pktlen,
                  uint8_t *type, uint32_t *fid,
                  const char **data, int *dlen);

//---------------------------------------------------------------------
// FNV-1a 32 位哈希（用于帧对账 / 完整性校验）。
//   服务器在广播帧负载末尾追加 4B 该哈希；客户端重算 body 比对，
//   不一致即判定该帧不同步。
//---------------------------------------------------------------------
uint32_t frame_fnv1a(const void *data, int len);

#ifdef __cplusplus
}
#endif

#endif /* KFS_FRAME_H */
