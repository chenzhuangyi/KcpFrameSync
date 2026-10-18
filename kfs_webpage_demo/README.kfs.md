# kfs 帧同步网页验证 —— 真连 Docker 服务器 运行说明

把 `LockstepDemo-master` 改造成了**真连你 Docker 里 kfs 帧同步服务器**的网页客户端，
链路为：

```
浏览器(WebSocket)  ⇄  gateway.js(本机 Node)  ⇄  UDP/KCP  ⇄  Docker kfs 服务器
```

- 浏览器没有 UDP socket，所以加了一个 Node 网关：每个浏览器连接 = 一个"虚拟 kfs 客户端"
  （独立 UDP socket + 独立 KCP 会话，行为对齐 C 客户端 `kfs_client_core.c`）。
- KCP 是 `ikcp.c` 的逐行转写 `gateway/ikcp.js`，与 C 版字节级兼容
  （conv/命令/头格式一致），已在回环测试 + 端到端测试中验证。
- 协议完全对齐你的库：裸 UDP JOIN 握手（room_id+conv+token 鉴权）、KCP 会话内
  INPUT/READY/HEARTBEAT/STATE/BROADCAST 帧、广播负载逐玩家 `[conv][len][data]` + 尾部
  FNV-1a32 对账哈希、输入提前量窗口（advance_frames=2）、客户端上报哈希
  `h=(h*31+byte)%2^32`。

---

## 一、启动顺序

### 1. Docker 容器内启动 kfs 服务器 + 网关（推荐）

容器已发布 `10500/udp`、`10500/tcp`、`18080/tcp`。在 Windows 执行：

```powershell
docker exec ctn_centos7 /root/kfs-gateway/start_container.sh
```

脚本会：
- 若 `server.lua` 没在跑，启动 `server.lua 1 1`（单人房，conv=101）；
- 启动**容器内网关** `ws://0.0.0.0:18080`，它直连容器内 `127.0.0.1:10500/udp`；
- 打印本次启动生成的 `tokens_1.txt`。

> **为什么网关要跑在容器里？**  
> Docker Desktop 的宿主机 UDP 端口代理会间歇性丢失，或只监听 IPv6 `[::]:10500`，
> 导致 Windows 上的网关 JOIN 超时。容器内网关用 loopback UDP，浏览器只走 TCP 18080，
> 完全绕开这个坑；TCP 映射比 UDP 稳定得多。
>
> 容器重启后需要重新执行一次上面的命令。

### 2. 起网页静态服务器（本机）

```powershell
cd D:\project\LockstepDemo-master\client
python -m http.server 8080 --bind 127.0.0.1
```

必须在 `client` 目录启动；从项目根目录启动时 `/` 会变成目录列表，没有 `index.html`。

### 3. 浏览器打开

```text
http://127.0.0.1:8080/?v=20261018d
```

**务必 Ctrl+F5 强刷**。登录页网关地址默认就是 `ws://127.0.0.1:18080`；token 会自动从
容器内 `tokens_1.txt` 读取，直接点蓝色「② 连接并对局」。

### 4. 可选：本机 Windows 网关（8081）

不想用容器内网关时：

```powershell
cd D:\project\LockstepDemo-master\gateway
node gateway.js
```

然后把网页里的网关地址改成 `ws://127.0.0.1:8081`。这条路径依赖 Docker Desktop 的
UDP 端口代理，可能间歇性超时；新版网关会自动尝试 IPv4/IPv6，但仍不如 18080 稳定。

---

## 二、登录联调

| 输入框 | 填什么 |
| --- | --- |
| 网关地址 | `ws://127.0.0.1:18080`（容器内网关，默认）；本机 8081 网关则改 `ws://127.0.0.1:8081` |
| 房间ID | server.lua 创建的房间号：`1`（或 1..房间数） |
| conv | 从服务器日志 / `tokens_<room_id>.txt` 抄（如 101） |
| token | 页面加载会自动读取当前值；也可点「① 读取当前 token」手动刷新 |
| 心跳间隔ms | `500`（对齐 config `heartbeat_ms`；网关自动补发 HEARTBEAT 帧） |
| 帧间隔ms | 页面会按服务器 `frame_rate` 自动设置（当前 `frame_rate=20` → `50`ms），一般不用手改 |

- **操作顺序**：打开页面 → 等自动填入 token（或点①）→ 点 **「② 连接并对局」**。
  token 为空时点②会自动读取再连接，不需要手动抄 token。
- **单人房**：`server.lua 1 1` → 只创建 conv=101；开一个标签页点②即可，
  服务器 1/1 就绪后直接推帧。
- **多人房**：`server.lua 1 2`（或更多）→ 开多个标签页，各填不同 conv + 对应 token，
  各自点②；全员 READY 后房间才 `start()` 推帧。
- **连接反馈**：canvas 上方有一条常驻状态条，依次显示
  `正在连接网关 → 网关已连接 → JOIN 成功 → 已收到广播 fid=N`；
  失败会变红并提示「返回登录 → ① → ②」。JOIN 成功但 2.5s 没广播会变黄提示检查 `room:start()`。
- 每个标签页即一个独立客户端：方向键发输入、数字键 1..N 切视角、
  `注入DESYNC`（偏移本地状态 → 上报哈希与全员不同 → 服务器对账报不一致）、
  `模拟掉线/重连`（断网关连接 → 服务器 idle_timeout 判掉线、席位保留 → 点"重连"恢复）、
  `错误token入房`（另开临时客户端用错误 token JOIN → 服务器应回 result=5）。
- **移动**：输入编码为 ASCII `'0'..'4'`（与 client.lua 一致）。页面会从网关读取
  `config` 的 `frame_rate` 并自动设置帧间隔（当前 `frame_rate=20` → `50`ms，
  4px/帧，80px/s）。所有玩家统一起点 x、按 conv 分行，保证同一方向移动距离一致；
  新客户端加入已在推帧的房间时会跳过旧帧，只补当前窗口，避免输入堵塞。
  两个标签页各自控制自己的 conv，按键前先点一下对应标签页让它获得焦点。
- 服务器推帧：server.lua 在全员就绪时 `room:start()`；只要 JOIN 成功就能看到广播帧。

---

## 三、WebSocket 消息协议（网关与浏览器之间）

浏览器 → 网关（文本 JSON，二进制字段用 hex）：
```
{op:"tokens", room_id}                             读取容器内当前 tokens_<room>.txt
{op:"join", room_id, conv, token, heartbeat_ms}   连接 + 发裸 UDP JOIN 握手
{op:"input", fid, data}                            上报一帧输入（data 为 hex）
{op:"ready"}                                       上报就绪（READY 帧）
{op:"state", fid, hash}                            上报某帧状态哈希（对账）
{op:"heartbeat"}  {op:"leave"}  {op:"ping"}
```
网关 → 浏览器：
```
{op:"tokens", room_id, tokens:[{conv,token}] | error}   当前 token（供登录框自动填）
{op:"joined", ok, result}                          result: 0成功 1不存在 2满 3冲突 4其他 5token错
{op:"broadcast", fid, payload, ok}                 广播帧（payload hex；ok=服务器对账哈希是否一致）
{op:"log", level, msg}  {op:"error", msg}  {op:"pong"}
```

---

## 四、网关配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `KFS_SERVER_HOST` | 自动 | kfs 服务器地址；不填则按 Windows UDP 监听自动选 IPv4/IPv6 |
| `KFS_SERVER_FAMILY` | `auto` | `udp4` / `udp6` / `auto`；Docker 只发布 `[::]:10500` 时用 `udp6` |
| `KFS_SERVER_PORT` | `10500` | kfs 服务器 UDP 端口 |
| `WS_PORT` | `8081` | 网关 WebSocket 端口（浏览器连接） |
| `KFS_UPDATE_MS` | `5` | 驱动节奏（对齐 C 客户端 1ms 节流） |
| `KFS_JOIN_TIMEOUT_MS` | `2000` | JOIN 应答等待超时（对齐 C 客户端 join 2s） |
| `KFS_DOCKER_CONTAINER` | `ctn_centos7` | 「读取当前 token」/doctor 使用的容器名 |
| `KFS_DOCKER_LUA_DIR` | `/root/project/KcpFrameSync/lua_test` | 容器内 `tokens_<room>.txt` 所在目录 |
| `KFS_LOCAL_TOKENS_DIR` | 空 | 网关跑在容器内时设为此目录，直接读本地 token（不调 docker） |

---

## 五、无需 Docker 的自测

```powershell
cd D:\project\LockstepDemo-master\gateway
$env:KFS_SERVER_PORT="10999"; $env:WS_PORT="8082"
node gateway.js            # 另开一个终端
node test_gateway.js       # 本地假 kfs 服务器验证整条链路
```
期望输出：`==== 端到端链路验证通过 ====  joined=true 对账通过帧数=3/3`。

---

## 六、已做的验证与本机限制

- **已验证**：`ikcp.js` 回环收发/分片重组（3000B 大包）；端到端链路
  JOIN→KCP→输入→广播对账 3/3 全过（假服务器）。
- **已验证（真实 Docker 服务器）**：两个浏览器虚拟客户端（conv=101/102）
  经网关 JOIN 真实 `server.lua`，READY 后收到 fid=0..2 广播，FNV-1a 对账全部一致。
  复现命令：`node test_real.js <101的token> <102的token>`。
- **已验证（双标签页移动）**：101/102 两个标签页同时按右方向键，位置从 x=62
  同步到 x=254，日志均为 `[c101:4 c102:4]`，移动距离一致。
- `gateway/gw_out.log`、`gw_err.log` 为早期测试残留，可删除。
- 服务器当前 `frame_rate=20`；页面会自动读取并设置帧间隔，无需手改。

---

## 七、JOIN 超时 / token 错误排查（重要）

> **推荐直接使用容器内网关 18080**（见第一节）。它走 loopback UDP，完全绕开下面描述的
> Docker Desktop UDP 端口代理问题；本节只针对“本机 Windows 网关 8081”这条可选路径。

### 症状 A：日志停在 `JOIN 已发送`，2s 后 `JOIN 应答超时`

这几乎都是**宿主机 → Docker 的 UDP 链路没通**，不是网关或 KCP 协议问题。
`docker ps` 里即使显示 `10500/udp` 映射正常，Windows 宿主机也可能没有对应的
UDP 监听（Docker Desktop 的端口代理丢失）。

在 Windows PowerShell 检查：

```powershell
netstat -ano -p UDP | findstr :10500
```

正常应有一行 `UDP 0.0.0.0:10500 ... <PID>`。**没有输出就是根因**，修复：

```powershell
docker restart ctn_centos7
# 容器重启后重新启动 server.lua（见下面“只保留一个 server.lua”）
```

### 症状 A2：有 UDP 监听，但网关仍 JOIN 超时（IPv6-only 发布）

Docker Desktop 有时把 `-p 10500:10500/udp` 发布成 **IPv6-only** 的 `[::]:10500`：
`netstat` 可能显示 `0.0.0.0:10500`，但 `Get-NetUDPEndpoint` 显示 `::`，此时
网关用 `127.0.0.1` + `udp4` 发 JOIN 收不到回包。

新版网关会按 `Get-NetUDPEndpoint` 自动选地址族，启动 banner 会显示：

```text
UDP 目标 : ::1:10500 (udp6, auto6)
```

如果仍显示 `127.0.0.1 (udp4)`，强制指定后重启网关：

```powershell
$env:KFS_SERVER_FAMILY="udp6"
node gateway.js
```

反之，如果 Docker 只发布 IPv4，可用 `KFS_SERVER_FAMILY="udp4"`。

一键诊断（在 `gateway` 目录）：

```powershell
node doctor.js
```

它会依次检查：Windows UDP 监听、Docker 端口映射、容器内 `server.lua` 数量、
当前 `tokens_<room>.txt`，并从宿主机发一个不会占用玩家席位的 JOIN 探测，直接指出卡在哪一步。

> `doctor.js` 的链路探测包故意用 `conv=0/token=0`，所以服务器日志会出现一行
> `kfs_srv_handle_join room_id=1, conv=0, token=0` 和 `token 错误` —— 这是诊断包，
> **不是 demo 的连接**。demo 正常 JOIN 的日志是 `conv=101, token=<当前值>`。
> 新版网关启动时不会再自己发这个探测包。

### 症状 B：`JOIN 应答 result=5(token错误)`

`token` 是 `server.lua` **每次启动**随机生成的，旧日志/旧页面里的 token 不能用。
读取本次启动生成的文件：

```powershell
docker exec ctn_centos7 cat /root/project/KcpFrameSync/lua_test/tokens_1.txt
```

把输出的 `conv token` 对应填到网页登录框。

### 症状 C：容器里有多个 `server.lua`

UDP + `SO_REUSEADDR` 会让内核只把单播包投给其中一个进程，而每个进程生成的 token
又不同，于是会出现“服务器明明在跑却超时 / token 对不上”的假象。

服务端 `kfs_src/common/net.c` 已改为**不设置 `SO_REUSEADDR`**：第二个实例会直接
bind 失败（`bind port 10500 failed (errno 98)`），不会再静默分流。

清理旧进程并只启动一个：

```powershell
docker exec ctn_centos7 sh -lc "pkill -f '[l]ua server.lua'"
docker exec -d ctn_centos7 bash -lc "cd /root/project/KcpFrameSync/lua_test && exec /root/project/KcpFrameSync/kfs_src/third/lua-5.4.8/src/lua server.lua 1 2 > server_run.log 2>&1"
```

> 修改过 `kfs_src/common/net.c` / `kfsserverlib/kfs_core.c` 后，需要在容器内
> `cd /root/project/KcpFrameSync/kfs_src && make` 重新生成 `.so`，再重启 `server.lua`。

---

## 八、Unity 开发：UDP↔TCP 桥（server 仍在 Docker Desktop）

Docker Desktop for Windows 的宿主机 UDP 端口代理不可靠（gVisor/VPNKit），
Unity 直接 UDP 连 `127.0.0.1:10500` 会间歇性超时。桥接方案不需要改服务端：

```text
Unity --UDP 127.0.0.1:10501--> bridge_windows.js
      --TCP 127.0.0.1:10500--> bridge_container.js（容器内）
      --UDP 127.0.0.1:10500--> server.lua
```

### 启动

```powershell
# 1. 容器内：server + 网页网关 + UDP/TCP 桥
docker exec ctn_centos7 /root/kfs-gateway/start_container.sh 1 1
# 双人房用 1 2

# 2. Windows：启动本机桥（保持这个终端开着）
cd D:\project\LockstepDemo-master\gateway
node bridge_windows.js
```

### Unity 侧

kfs 客户端只改目标端口：

```csharp
c.setpeer("127.0.0.1", 10501);   // 原来是 10500
```

房间、conv、token、JOIN/KCP/心跳/广播协议都不变。

### 端口

| 段 | 协议/端口 | 说明 |
|---|---|---|
| Unity → Windows 桥 | UDP `127.0.0.1:10501` | `bridge_windows.js` 监听 |
| Windows 桥 → 容器桥 | TCP `127.0.0.1:10500` | Docker 已发布，稳定 |
| 容器桥 → server | UDP `127.0.0.1:10500` | 容器内 loopback |

### 多客户端

Windows 桥按 Unity 的源 `ip:port` 分别建 TCP 连接；容器桥为每条连接建独立
UDP socket。服务器看到的是多个不同 UDP 源，JOIN/KCP 按 conv 正常工作。
每个 Unity 客户端仍需自己的 conv/token（和直连时一样）。

### 注意

- `bridge_windows.js` 必须保持运行；Unity 断开后桥会在空闲 5 分钟后清理映射。
- 容器重启后重新执行 `start_container.sh`；Windows 桥不用重启，下个 UDP 包会自动重连。
- 桥重启会让正在进行的 KCP 会话断开，Unity 客户端需要重新 JOIN。
- 想验证桥是否通：
  ```powershell
  node test_bridge_kcp.js <当前token>
  ```
  收到 `JOIN ACK result=0` 和 `BROADCAST ... data=34` 即成功。
