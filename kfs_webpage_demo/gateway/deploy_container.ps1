#=====================================================================
# deploy_container.ps1 - 把本机 gateway 目录里的网关文件同步进容器，
# 并重启容器内网关（ws://0.0.0.0:18080）。
#
# 用法（在 gateway 目录）：
#   pwsh -ExecutionPolicy Bypass -File .\deploy_container.ps1
#   powershell -ExecutionPolicy Bypass -File .\deploy_container.ps1
#
# 环境变量：
#   KFS_DOCKER_CONTAINER  容器名，默认 ctn_centos7
#   KFS_CONTAINER_ROOT    容器内项目根目录，默认 /root/project/KcpFrameSync
#
# 容器内网关直连 127.0.0.1:<config 的 udp_port>/udp，不依赖 Docker Desktop
# 的宿主机 UDP 端口代理；浏览器只走 TCP 18080。
#=====================================================================
$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path

$container = if ($env:KFS_DOCKER_CONTAINER) { $env:KFS_DOCKER_CONTAINER } else { 'ctn_centos7' }
$containerRoot = if ($env:KFS_CONTAINER_ROOT) { $env:KFS_CONTAINER_ROOT } else { '/root/project/KcpFrameSync' }
$gatewayDest = "$containerRoot/kfs-gateway"

Write-Output "容器: $container"
Write-Output "目标: ${container}:$gatewayDest"

docker exec $container mkdir -p "$gatewayDest/node_modules"
foreach ($f in @('gateway.js', 'ikcp.js', 'start_container.sh')) {
    if (-not (Test-Path (Join-Path $here $f))) {
        throw "缺少文件: $(Join-Path $here $f)"
    }
    docker cp "$here\$f" "${container}:$gatewayDest/$f"
}

docker exec $container rm -rf "$gatewayDest/node_modules/ws"
docker cp "$here\node_modules\ws" "${container}:$gatewayDest/node_modules/ws"
docker exec $container chmod +x "$gatewayDest/start_container.sh"

# 只重启网关；server.lua 保持运行（token 不变，网页里已有的 token 继续有效）
docker exec -e RESTART_GATEWAY=1 $container "$gatewayDest/start_container.sh"

Write-Output ""
Write-Output "容器内网关已更新: ws://127.0.0.1:18080"
Write-Output "容器内已移除 UDP/TCP 中转桥；网关直连 udp/<udp_port>"
Write-Output "网页网关地址填 ws://127.0.0.1:18080；Unity 直连服务器 UDP 端口 setpeer('127.0.0.1', 10500)。"
