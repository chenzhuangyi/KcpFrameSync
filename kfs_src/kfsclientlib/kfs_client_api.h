//=====================================================================
// kfs_client_api.h — KCP 帧同步客户端库 · 公共 C ABI（语言无关门面层）
//
// 定位：业务层 / 宿主框架（Unity C#、Go、Rust、Cocos、xLua…）唯一
//   需要看到的头文件。
//   - 不依赖 Lua / pthread / socket 类型，只依赖 stdint/stddef。
//   - 句柄不透明（struct kfs_client 仅前向声明），布局属于库内部。
//   - 驱动线程、KCP 喂包与重传、收帧解帧、对账校验、事件队列、
//     自动心跳、追帧、握手与时钟锚定 —— 全部封装在本 ABI 之后；
//     宿主每帧只做一件事：poll 事件。
//
// 线程模型：库内部自带驱动线程；本 ABI 所有函数内部加锁，
//   宿主可在任意线程调用（典型用法：渲染主线程每帧 poll_event）。
//
// 事件内存规则：poll_* / catch_up 取出的事件负载（ev.data）为库分配的
//   堆内存，使用完必须调用 kfs_client_event_release 归还，否则泄漏。
//
// 确定性边界：本层只负责"字节何时到、帧是否完整一致"；
//   帧节拍推进、锁步等待、仿真计算属于宿主，不在本层。
//=====================================================================
#ifndef KFS_CLIENT_API_H
#define KFS_CLIENT_API_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------------------------------------------
// 导出符号（默认隐藏可见性构建下，仅这些符号对外可见）
//   编译本库时定义 KFS_CLIENT_EXPORTS；宿主引用本头时不定义（Windows
//   下自动切 dllimport）。非 Windows 依赖 visibility=hidden + 本宏。
//---------------------------------------------------------------------
#if defined(_WIN32)
  #if defined(KFS_CLIENT_EXPORTS)
    #define KFS_CAPI __declspec(dllexport)
  #else
    #define KFS_CAPI __declspec(dllimport)
  #endif
#else
  #if defined(KFS_CLIENT_EXPORTS)
    #define KFS_CAPI __attribute__((visibility("default")))
  #else
    #define KFS_CAPI
  #endif
#endif

//---------------------------------------------------------------------
// 不透明句柄（内部定义见 kfs_client.h，宿主不可见）
//---------------------------------------------------------------------
typedef struct kfs_client kfs_client;

//---------------------------------------------------------------------
// 状态码（0=成功，负数=错误；错误详情用 kfs_client_last_error 查询）
//---------------------------------------------------------------------
typedef enum {
    KFS_OK              =  0,
    KFS_ERR             = -1,   // 通用错误
    KFS_ERR_CLOSED      = -2,   // 客户端已关闭
    KFS_ERR_NOT_JOINED  = -3,   // 未加入房间（须先 join 成功）
    KFS_ERR_NO_PEER     = -4,   // 未设置服务器地址（须先 set_peer）
    KFS_ERR_TIMEOUT     = -5,   // join 等待应答超时
    KFS_ERR_REJECTED    = -6,   // 服务器拒绝 join（result 见 last_error）
    KFS_ERR_PARAM       = -7,   // 参数非法（ip 解析失败/编码失败等）
    KFS_ERR_NOMEM       = -8,   // 内存分配失败
    KFS_ERR_PENDING     = -9    // join_start 后应答尚未到达
} kfs_client_status;

//---------------------------------------------------------------------
// 事件类型（驱动线程解帧后入队，宿主 poll 取走）
//---------------------------------------------------------------------
typedef enum {
    KFS_EVENT_FRAME       = 1,  // 服务器广播帧（data=完整负载，含末尾4B对账哈希；
                                //   consistent=1 表示与服务器重算哈希一致）
    KFS_EVENT_ROOM_START  = 2   // 全员就绪开战信号（fid=起始帧号）
} kfs_client_event_type;

// 事件取出结构：type/fid/consistent/len 为值；data 为库分配的堆内存，
// 用完必须 kfs_client_event_release(&ev)。
typedef struct {
    int      type;        // kfs_client_event_type
    uint32_t fid;         // 帧号（服务器权威分配）
    int      consistent;  // 仅 FRAME 有意义：1=本帧全员输入哈希对账一致
    int      len;         // data 字节数（可为 0）
    uint8_t *data;        // 负载副本（malloc；release 负责 free）
} kfs_client_event;

//---------------------------------------------------------------------
// 生命周期
//---------------------------------------------------------------------
// 创建会话并启动内部驱动线程。
//   local_port : 本机 UDP 端口（0=系统分配）
//   conv       : KCP 会话号（与服务器 add_player 登记一致）
//   err_out    : 可为 NULL；失败时接收错误描述字符串（静态存储，勿 free）
// 成功返回句柄；失败返回 NULL。
KFS_CAPI kfs_client *kfs_client_create(uint16_t local_port, uint32_t conv,
                                       const char **err_out);

// 停止驱动线程、关闭 socket、释放 KCP 与事件队列（幂等，可传 NULL）。
KFS_CAPI void kfs_client_destroy(kfs_client *c);

// 最近一次失败的描述（供报错/日志用）。
KFS_CAPI const char *kfs_client_last_error(kfs_client *c);

// 库版本标识。
KFS_CAPI const char *kfs_client_version(void);

//---------------------------------------------------------------------
// 连接与登录
//---------------------------------------------------------------------
// 指定服务器地址。返回 KFS_OK / KFS_ERR_PARAM(ip 非法) / KFS_ERR_CLOSED
KFS_CAPI int kfs_client_set_peer(kfs_client *c, const char *ip, uint16_t port);

// 加入房间（阻塞版：发送裸 UDP JOIN 并等待 ACK，5s 超时）。
//   token: 会话鉴权令牌（0=不校验，须与服务器配置一致）
// 返回 KFS_OK / KFS_ERR_NO_PEER / KFS_ERR_REJECTED / KFS_ERR_TIMEOUT。
KFS_CAPI int kfs_client_join(kfs_client *c, uint32_t room_id, uint32_t token);

// 加入房间（异步版，供宿主主线程使用，不阻塞）：
//   join_start 发送 JOIN 请求；随后每帧调 join_check 直到不再是 PENDING。
//   成功 ACK 中的帧间隔/随机种子/服务器时间由驱动线程自动锚定（见下行查询）。
KFS_CAPI int kfs_client_join_start(kfs_client *c, uint32_t room_id, uint32_t token);
KFS_CAPI int kfs_client_join_check(kfs_client *c);

//---------------------------------------------------------------------
// 业务上行（帧编码在本层完成，宿主只见语义接口）
//---------------------------------------------------------------------
// 上报一帧输入（KFS_CMD_INPUT）。len 上限 0xFFFF。
KFS_CAPI int kfs_client_send_input(kfs_client *c, uint32_t frame_id,
                                   const void *data, int len);

// 上报就绪（KFS_CMD_READY）：游戏数据加载完毕后调用，全员就绪后服务器推帧。
KFS_CAPI int kfs_client_send_ready(kfs_client *c);

// 上报某帧逻辑状态哈希（KFS_CMD_STATE），供服务器对账推进一致点。
KFS_CAPI int kfs_client_report_state(kfs_client *c, uint32_t frame_id,
                                     uint32_t state_hash);

//---------------------------------------------------------------------
// 事件取出（下行）
//---------------------------------------------------------------------
// 取下一个事件。返回 1=已填充 ev；0=队列空；-1=客户端已关闭。
// 所有事件类型都会取出，由宿主自行分发。
KFS_CAPI int kfs_client_poll_event(kfs_client *c, kfs_client_event *ev);

// 便捷版：只取广播帧事件，途中其他类型事件自动分发记账
// （ROOM_START 会置 started 标志）并释放。语义等价旧 Lua API recv_broadcast。
// 返回 1=有帧；0=无；-1=已关闭。
KFS_CAPI int kfs_client_poll_frame(kfs_client *c, kfs_client_event *ev);

// 追帧：清空积压事件，只保留最新一帧广播（严重滞后时跳到服务器当前帧）。
// 返回 1=有帧；0=无；-1=已关闭。
KFS_CAPI int kfs_client_catch_up(kfs_client *c, kfs_client_event *ev);

// 归还 poll_* / catch_up 取出的事件负载（内部 free 并清零指针，幂等）。
KFS_CAPI void kfs_client_event_release(kfs_client_event *ev);

// 当前待取事件数（积压观测用，>0 说明宿主消费慢于网络）。
KFS_CAPI int kfs_client_pending_events(kfs_client *c);

//---------------------------------------------------------------------
// 会话参数查询 / 调优
//---------------------------------------------------------------------
// 以下三项在 join 成功后由驱动线程自动锚定（ACK 未达时返回 0 / 未定义）：
KFS_CAPI uint32_t kfs_client_frame_interval_ms(kfs_client *c);  // 帧间隔
KFS_CAPI uint32_t kfs_client_room_seed(kfs_client *c);          // 房间随机种子
// 服务器与本地时钟差（ms）：本地单调时刻 = 服务器时刻 + offset。
KFS_CAPI int64_t  kfs_client_server_time_offset_ms(kfs_client *c);

// 服务器最近一次开战的起始帧号（未收到开战信号时返回 0）。
KFS_CAPI uint32_t kfs_client_start_fid(kfs_client *c);

// 是否已收到全员就绪开战信号（ALL_READY）。1=已开战，0=未。
KFS_CAPI int kfs_client_is_started(kfs_client *c);

// 设置自动心跳间隔（ms，0=关闭）。驱动线程按此定期补发 HEARTBEAT。
KFS_CAPI int kfs_client_set_heartbeat(kfs_client *c, int ms);

// KCP 调优透传（语义同 ikcp_nodelay / ikcp_wndsize / ikcp_setmtu）。
KFS_CAPI int kfs_client_nodelay(kfs_client *c, int nodelay, int interval,
                                int resend, int nc);
KFS_CAPI int kfs_client_wndsize(kfs_client *c, int sndwnd, int rcvwnd);
KFS_CAPI int kfs_client_setmtu(kfs_client *c, int mtu);

// 是否已关闭。1=已关闭，0=存活。
KFS_CAPI int kfs_client_is_closed(kfs_client *c);

#ifdef __cplusplus
}
#endif

#endif /* KFS_CLIENT_API_H */
