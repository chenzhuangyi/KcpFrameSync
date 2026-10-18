#!/bin/bash
#=====================================================================
# start_container.sh — 在 ctn_centos7 容器内启动 kfs 服务器 + 容器内网关
#
# 用法（Windows）：
#   docker exec ctn_centos7 /root/project/KcpFrameSync/kfs-gateway/start_container.sh
#   docker exec ctn_centos7 /root/project/KcpFrameSync/kfs-gateway/start_container.sh 1 1   # 单人房
#   docker exec ctn_centos7 /root/project/KcpFrameSync/kfs-gateway/start_container.sh 1 2   # 1 房 2 人
#
# 环境变量：
#   START_GATEWAY=0     只起服务器，不起网页网关
#   RESTART_GATEWAY=1   先杀旧 gateway.js / bridge_container.js 再起（改过网关代码或端口后用）
#   KFS_SERVER_PORT     覆盖网关连的 UDP 端口（默认读 lua_test/config 的 udp_port，再退回 10500）
#
# 重要：脚本按“自身所在目录”定位 ../kfs_src 和 ../lua_test，不再依赖当前工作目录。
# 之前用相对路径 ../ 时，docker exec 的工作目录是 /，会把 ../lua_test 解析成
# /lua_test 而直接退出，表现为“18080 没有网关、网页提示网关未启动”。
#=====================================================================
set -u

ROOMS=${1:-1}
PLAYERS=${2:-2}
START_GATEWAY=${START_GATEWAY:-1}
RESTART_GATEWAY=${RESTART_GATEWAY:-0}

SCRIPT_DIR=$(cd "$(dirname "$0")" 2>/dev/null && pwd) || {
    echo "无法定位脚本目录: $0"
    exit 1
}
ROOT_DIR=$(cd "$SCRIPT_DIR/.." 2>/dev/null && pwd) || {
    echo "无法定位项目根目录: $SCRIPT_DIR/.."
    exit 1
}

NODE=/usr/local/bin/kfs-node
if [ ! -x "$NODE" ]; then
    NODE=$(command -v node 2>/dev/null || true)
fi
if [ -z "${NODE:-}" ] || [ ! -x "$NODE" ]; then
    echo "找不到 node（/usr/local/bin/kfs-node 或 PATH 里的 node）"
    exit 1
fi

LUA="$ROOT_DIR/kfs_src/third/lua-5.4.8/src/lua"
LUA_DIR="$ROOT_DIR/lua_test"
GW_DIR="$SCRIPT_DIR"

if [ ! -x "$LUA" ]; then
    echo "找不到 lua（$LUA）"
    exit 1
fi
if [ ! -f "$LUA_DIR/server.lua" ]; then
    echo "找不到 $LUA_DIR/server.lua"
    exit 1
fi
if [ ! -f "$GW_DIR/gateway.js" ]; then
    echo "找不到 $GW_DIR/gateway.js"
    exit 1
fi

# 网关目标端口必须与服务器实际监听端口一致：
#   优先 KFS_SERVER_PORT，其次 lua_test/config 的 udp_port，最后 10500。
UDP_PORT=${KFS_SERVER_PORT:-}
if [ -z "$UDP_PORT" ]; then
    UDP_PORT=$(awk -F= '/^[[:space:]]*udp_port[[:space:]]*=/{gsub(/[^0-9]/, "", $2); print $2; exit}' \
        "$LUA_DIR/config" 2>/dev/null)
fi
UDP_PORT=${UDP_PORT:-10500}

echo "项目根目录: $ROOT_DIR"
echo "UDP 端口  : $UDP_PORT"

#---------------------------------------------------------------------
# 1) kfs 服务器（server.lua）
#---------------------------------------------------------------------
cd "$LUA_DIR" || exit 1
if ! pgrep -f '[l]ua server.lua' >/dev/null; then
    rm -f tokens_*.txt server_run.log
    nohup "$LUA" server.lua "$ROOMS" "$PLAYERS" > server_run.log 2>&1 &
    sleep 1
    if pgrep -f '[l]ua server.lua' >/dev/null; then
        echo "server.lua 已启动（$ROOMS 房 / 每房 $PLAYERS 人）"
    else
        echo "server.lua 启动失败："
        tail -n 20 server_run.log 2>/dev/null
        exit 1
    fi
else
    echo "server.lua 已在运行（不会改变房间参数）"
fi

#---------------------------------------------------------------------
# 2) 容器内网页网关（ws://0.0.0.0:18080，直连 127.0.0.1:$UDP_PORT）
#---------------------------------------------------------------------
if [ "$START_GATEWAY" = "1" ]; then
    if [ "$RESTART_GATEWAY" = "1" ]; then
        pkill -f '[g]ateway.js' >/dev/null 2>&1 || true
        sleep 0.3
    fi
    if pgrep -f '[g]ateway.js' >/dev/null; then
        echo "容器内网关已在运行（改过代码请用 RESTART_GATEWAY=1 重启）"
    else
        cd "$GW_DIR" || exit 1
        WS_PORT=18080 \
        KFS_SERVER_HOST=127.0.0.1 \
        KFS_SERVER_FAMILY=udp4 \
        KFS_SERVER_PORT="$UDP_PORT" \
        KFS_LOCAL_TOKENS_DIR="$LUA_DIR" \
        nohup "$NODE" gateway.js > gateway.log 2>&1 &
        GW_PID=$!
        sleep 1
        if kill -0 "$GW_PID" 2>/dev/null; then
            echo "容器内网关已启动 ws://0.0.0.0:18080 (pid $GW_PID, UDP 目标 127.0.0.1:$UDP_PORT)"
        else
            echo "容器内网关启动失败："
            tail -n 20 gateway.log 2>/dev/null
        fi
    fi
else
    echo "跳过网页网关（START_GATEWAY=0）；Unity 仍可使用 TCP 10500 桥"
fi

#---------------------------------------------------------------------
# 3) Unity 用的 UDP↔TCP 桥：Windows 桥 -> tcp/10500 -> udp/$UDP_PORT
#---------------------------------------------------------------------
if [ "$RESTART_GATEWAY" = "1" ]; then
    pkill -f '[b]ridge_container.js' >/dev/null 2>&1 || true
    sleep 0.2
fi
if ! pgrep -f '[b]ridge_container.js' >/dev/null; then
    cd "$GW_DIR" || exit 1
    BRIDGE_TCP_PORT=10500 \
    BRIDGE_SERVER_HOST=127.0.0.1 \
    BRIDGE_SERVER_PORT="$UDP_PORT" \
    nohup "$NODE" bridge_container.js > bridge.log 2>&1 &
    sleep 0.5
    echo "容器内 UDP/TCP 桥已启动 tcp/10500 -> udp 127.0.0.1:$UDP_PORT"
else
    echo "容器内 UDP/TCP 桥已在运行"
fi

echo "--- tokens ---"
cat "$LUA_DIR/tokens_1.txt" 2>/dev/null || echo "tokens_1.txt 尚未生成"
