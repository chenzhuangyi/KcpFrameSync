//=====================================================================
// log.c — 临时日志实现
//=====================================================================
#include "kfs_common.h"
#include "log.h"

#include <stdio.h>
#include <string.h>
#include <stdarg.h>


// LOG_DEBUGF("Hello, world! %d", 123);
// [2026-10-18 17:57:04][DEBUG][C][kfs_lua.c:81] Hello, world! 123
void kfs_logf(const char *level, const char *file, int line, const char *fmt, ...) {
    if (strcmp(level, LOG_LEVEL_DEBUG) < 0 || strcmp(level, LOG_LEVEL_NONE) > 0) return;

    va_list ap;
    va_start(ap, fmt);

    printf("[%s][%s][C][%s:%d] ", now_datetime(), level, file, line);
    vprintf(fmt, ap);
    printf("\n");

    va_end(ap);
    fflush(stdout);
}
