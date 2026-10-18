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

// xorshift64* 伪随机数（生成玩家 token；非加密级，原型够用）
uint32_t kfs_rng_next(void);


#define LOG_LEVEL_DEBUG "DEBUG"     // 调试日志
#define LOG_LEVEL_INFO  "INFO"      // 信息日志
#define LOG_LEVEL_WARN  "WARN"      // 警告日志
#define LOG_LEVEL_ERROR "ERROR"     // 错误日志
#define LOG_LEVEL_FATAL "FATAL"     // 严重错误日志
#define LOG_LEVEL_NONE  "NONE"      // 无日志

// 打印日志，打印[server][C][[日志级别][日期时间][文件名：行号] 日志内容（日志级别：DEBUG、INFO、WARN、ERROR、FATAL、NONE）
// 示例：LOG_DEBUGF("Hello, world! %d", 123);
#define LOG_DEBUGF(fmt, ...) \
    logf(LOG_LEVEL_DEBUG, \
         strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__, \
         __LINE__, \
         fmt, ##__VA_ARGS__)


void logf(const char *level, const char *file, int line, const char *fmt, ...);

#ifdef __cplusplus
}
#endif

#endif /* KFS_COMMON_H */
