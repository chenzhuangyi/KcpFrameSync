//=====================================================================
// log.h — 临时日志
//
// 
//=====================================================================
#ifndef KFS_LOG_H
#define KFS_LOG_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif


#define LOG_LEVEL_DEBUG "DEBUG"     // 调试日志
#define LOG_LEVEL_INFO  "INFO"      // 信息日志
#define LOG_LEVEL_WARN  "WARN"      // 警告日志
#define LOG_LEVEL_ERROR "ERROR"     // 错误日志
#define LOG_LEVEL_FATAL "FATAL"     // 严重错误日志
#define LOG_LEVEL_NONE  "NONE"      // 无日志

// 打印日志，打印[server][C][[日志级别][日期时间][文件名：行号] 日志内容（日志级别：DEBUG、INFO、WARN、ERROR、FATAL、NONE）
// 示例：LOG_DEBUGF("Hello, world! %d", 123);
#define LOG_DEBUGF(fmt, ...) \
    kfs_logf(LOG_LEVEL_DEBUG, \
         strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__, \
         __LINE__, \
         fmt, ##__VA_ARGS__)


void kfs_logf(const char *level, const char *file, int line, const char *fmt, ...);

#ifdef __cplusplus
}
#endif

#endif /* KFS_LOG_H */
