#!/usr/bin/env bash
#=====================================================================
# stop_all.sh — 停止 KCP 帧同步测试的全部相关进程
#
# 用途：
#   一键停止 KCP 帧同步测试脚本（server.lua / client.lua），
#   用于在 Linux 上测试后清理，或中断卡住的测试进程。
#
# 兼容性：
#   - 从任意目录调用均可（脚本自动定位自身所在目录）。
#   - 按进程命令行匹配 server.lua / client.lua，只杀测试进程，
#     不会误杀系统里其他无关的 lua 进程。
#   - 幂等：没有相关进程时安全退出，返回码 0。
#
# 用法：
#   chmod +x stop_all.sh
#   ./stop_all.sh
#   # 强制模式（-9，杀不掉时用）：
#   ./stop_all.sh -f
#=====================================================================
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
FORCE=""
if [ "${1:-}" = "-f" ]; then
    FORCE="-9"
    echo "== 强制模式：使用 SIGKILL =="
fi

echo "== 停止 KCP 帧同步测试进程（脚本目录: $HERE） =="
killed=0

# 匹配的测试脚本名（命令行里含这些字符串的 lua 进程会被停止）
for pat in "server.lua" "client.lua"; do
    while read -r pid; do
        [ -n "$pid" ] || continue
        # 双保险：跳过本脚本自身（pgrep -f 可能匹配到 stop_all.sh 的注释/参数）
        cmd="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)"
        case "$cmd" in
            *stop_all.sh*) continue ;;
        esac
        echo "  停止 $pat  (pid=$pid)"
        kill $FORCE "$pid" 2>/dev/null || true
        killed=$((killed + 1))
    done < <(pgrep -f "$pat" || true)
done

# 等进程真正退出（-9 之前先给一次优雅退出机会）
if [ "$killed" -gt 0 ] && [ -z "$FORCE" ]; then
    sleep 1
fi

if [ "$killed" -eq 0 ]; then
    echo "  没有找到相关进程，已全部清理。"
else
    echo "  已发出停止信号，共 $killed 个进程。"
fi
echo "== 完成 =="
