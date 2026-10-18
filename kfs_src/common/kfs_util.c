//=====================================================================
// kfs_util.c — 服务器与客户端共享的公共工具实现
//=====================================================================
#include "kfs_common.h"

#include <stdio.h>
#include <time.h>
#include <stdarg.h>

// 当前单调时钟毫秒数
IUINT32 kfs_now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (IUINT32)((uint64_t)ts.tv_sec * 1000u
                     + (uint64_t)ts.tv_nsec / 1000000u);
}

// xorshift64* 伪随机数生成（用于生成玩家会话 token）。
//   非加密级（原型够用）；种子取单调时钟 + 自身地址，保证每次运行不同。
static uint64_t kfs_rng_state = 0;

uint32_t kfs_rng_next(void)
{
    uint64_t x;
    if (kfs_rng_state == 0) {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        kfs_rng_state = (uint64_t)ts.tv_sec * 1000000000ull
                      + (uint64_t)ts.tv_nsec;
        kfs_rng_state ^= (uint64_t)(uintptr_t)&kfs_rng_state;
        if (kfs_rng_state == 0)
            kfs_rng_state = 0x9E3779B97F4A7C15ull;
    }
    x = kfs_rng_state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    kfs_rng_state = x;
    return (uint32_t)((x * 0x2545F4914F6CDD1Dull) >> 32);
}

const char* now_datetime(void) {
    static char buf[64];
    time_t t = time(NULL);
    struct tm *lt = localtime(&t);
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", lt);
    return buf;
}

// LOG_DEBUGF("Hello, world! %d", 123);
// [2026-10-18 17:57:04][DEBUG][C][kfs_lua.c:81] Hello, world! 123
void logf(const char *level, const char *file, int line, const char *fmt, ...) {
    if (strcmp(level, LOG_LEVEL_DEBUG) < 0 || strcmp(level, LOG_LEVEL_NONE) > 0) return;

    va_list ap;
    va_start(ap, fmt);

    printf("[%s][%s][C][%s:%d] ", now_datetime(), level, file, line);
    vprintf(fmt, ap);
    printf("\n");

    va_end(ap);
    fflush(stdout);
}

