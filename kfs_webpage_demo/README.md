## 帧同步网页demo
帧同步的客户端示例（浏览器 ⟷ WebSocket 网关 ⟷ UDP/KCP ⟷ Docker 内 kfs 服务器）。

## 目录
- `kfs_webpage_demo/client`：网页客户端（`index.html` / `main.js`）
- `kfs_webpage_demo/gateway`：Windows 本机网关（8081）、Unity 桥、诊断脚本
- `kfs-gateway`：部署到容器的网关目录（容器内 `/root/project/KcpFrameSync/kfs-gateway`）
- `kfs_src`：kfs 帧同步库
- `lua_test`：`server.lua` / `config`

## 如何运行

### 1. 推荐：容器内网关（18080）

```shell
docker exec ctn_centos7 /root/project/KcpFrameSync/kfs-gateway/start_container.sh
# Windows 也可以直接运行 kfs-gateway\start_container.bat
```

脚本会启动容器里的 `server.lua`、网页网关（`ws://0.0.0.0:18080`）和 Unity 用的
TCP/UDP 桥，并打印当前 `tokens_1.txt`。改过网关代码后用：

```shell
docker exec -e RESTART_GATEWAY=1 ctn_centos7 /root/project/KcpFrameSync/kfs-gateway/start_container.sh
```

或从 Windows 部署并重启容器网关：

```powershell
cd kfs_webpage_demo\gateway
powershell -ExecutionPolicy Bypass -File .\deploy_container.ps1
```

### 2. 本机网关（8081，可选）

```shell
cd kfs_webpage_demo\gateway
npm install
node gateway.js
```

网页里的网关地址改成 `ws://127.0.0.1:8081`。

### 3. 静态服务器

```shell
cd kfs_webpage_demo\client
python -m http.server 8080 --bind 127.0.0.1
```

打开 [http://127.0.0.1:8080/](http://127.0.0.1:8080/)，先点「① 读取当前 token」，
再点「② 连接并对局」。

## 排查

```powershell
cd kfs_webpage_demo\gateway
node doctor.js
```

- `读取 token 失败：网关未启动？` = 浏览器连不上 WebSocket：18080 要先启动容器网关，
  8081 要先 `node gateway.js`。Docker 的 TCP 代理会“接受连接”，所以 18080 能连上
  不代表容器里真有网关。
- `JOIN 应答超时` = 网关到服务器的 UDP 链路问题：确认 `server.lua` 在跑、网关目标端口
  与 `lua_test/config` 的 `udp_port` 一致（默认 10500）。

## 示例截图
![](README/preview.png)
