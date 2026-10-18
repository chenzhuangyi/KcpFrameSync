#=====================================================================
# deploy_container.ps1 - sync gateway.js / ikcp.js into the container
# and restart the in-container gateway (ws://0.0.0.0:18080).
#
# Usage (from the gateway directory):
#   pwsh -ExecutionPolicy Bypass -File .\deploy_container.ps1
#   or: powershell -ExecutionPolicy Bypass -File .\deploy_container.ps1
#
# The in-container gateway talks to 127.0.0.1:10500/udp directly and
# does not depend on Docker Desktop's host UDP port proxy.
#=====================================================================
$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$container = 'ctn_centos7'

docker exec $container mkdir -p /root/kfs-gateway/node_modules
docker cp "$here\gateway.js" "${container}:/root/kfs-gateway/gateway.js"
docker cp "$here\ikcp.js" "${container}:/root/kfs-gateway/ikcp.js"
docker cp "$here\bridge_container.js" "${container}:/root/kfs-gateway/bridge_container.js"
docker exec $container rm -rf /root/kfs-gateway/node_modules/ws
docker cp "$here\node_modules\ws" "${container}:/root/kfs-gateway/node_modules/ws"
docker cp "$here\start_container.sh" "${container}:/root/kfs-gateway/start_container.sh"
docker exec $container chmod +x /root/kfs-gateway/start_container.sh

# Stop old gateway/bridge only; server.lua keeps running.
docker exec $container sh -lc "pkill -f '[g]ateway.js' || true; pkill -f '[b]ridge_container.js' || true"
Start-Sleep -Milliseconds 500
docker exec $container /root/kfs-gateway/start_container.sh

Write-Output ""
Write-Output "Container gateway updated: ws://127.0.0.1:18080"
Write-Output "Container Unity bridge updated: tcp/10500 -> udp/10500"
Write-Output "Web: set gateway ws://127.0.0.1:18080. Unity: setpeer('127.0.0.1', 10501) with bridge_windows.js running."
