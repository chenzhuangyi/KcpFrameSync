#!/bin/bash
#=====================================================================
# start_container.sh — 在 ctn_centos7 容器内启动 kfs 服务器 + 容器内网关
#
# 用法（Windows）：
#   docker exec ctn_centos7 /root/kfs-gateway/start_container.sh
#
# 容器重启后执行一次即可。网关监听 18080/tcp（Docker 已发布），
# 容器内部直连 127.0.0.1:10500/udp，不依赖 Docker Desktop 的 UDP 端口代理。
#=====================================================================
set -u

NODE=/usr/local/bin/kfs-node
[ -x "$NODE" ] || NODE=$(command -v node)

LUA=/root/project/KcpFrameSync/kfs_src/third/lua-5.4.8/src/lua
LUA_DIR=/root/project/KcpFrameSync/lua_test
GW_DIR=/root/kfs-gateway

if [ ! -x "$NODE" ]; then
    echo "找不到 node（$NODE）"
    exit 1
fi

if [ ! -x "$LUA" ]; then
    echo "找不到 lua（$LUA）"
    exit 1
fi

cd "$LUA_DIR" || exit 1
if ! pgrep -f '[l]ua server.lua' >/dev/null; then
    rm -f tokens_1.txt server_run.log
    nohup "$LUA" server.lua 1 1 > server_run.log 2>&1 &
    sleep 1
    echo "server.lua 已启动（1 1）"
else
    echo "server.lua 已在运行"
fi

cd "$GW_DIR" || exit 1
if ! pgrep -f '[g]ateway.js' >/dev/null; then
    WS_PORT=18080 \
    KFS_SERVER_HOST=127.0.0.1 \
    KFS_SERVER_FAMILY=udp4 \
    KFS_LOCAL_TOKENS_DIR="$LUA_DIR" \
    nohup "$NODE" gateway.js > gateway.log 2>&1 &
    sleep 1
    echo "容器内网关已启动 ws://0.0.0.0:18080"
else
    echo "容器内网关已在运行"
fi

echo "--- tokens ---"
cat "$LUA_DIR/tokens_1.txt" 2>/dev/null || echo "tokens_1.txt 尚未生成"
