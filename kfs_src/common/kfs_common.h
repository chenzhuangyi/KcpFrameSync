//=====================================================================
// kfs_common.h — 服务器与客户端共享的公共工具声明
//
// 服务器库（server/kfs_*.c）与客户端库（client/kfs_client_*.c）共用：
//   - kfs_now_ms()  单调时钟毫秒（驱动/超时/掉线判定）
//   - kfs_rng_next() xorshift64* 伪随机（生成玩家会话 token）
// 实现见 common/kfs_util.c。
//=====================================================================
#ifndef KFS_COMMON_H
#define KFS_COMMON_H

#include <stdint.h>
#include "kcp/ikcp.h"

#ifdef __cplusplus
extern "C" {
#endif

// 当前单调时钟毫秒数（驱动节拍 / 超时 / 掉线判定共用）
IUINT32 kfs_now_ms(void);

const char* now_datetime(void);

// xorshift64* 伪随机数（生成玩家 token；非加密级，原型够用）
uint32_t kfs_rng_next(void);


#ifdef __cplusplus
}
#endif

#endif /* KFS_COMMON_H */
