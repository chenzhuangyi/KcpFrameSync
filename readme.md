# KCP 帧同步库 — kfsserverlib / kfsclientlib（服务器 + 客户端两个 .so）

把 KCP 帧同步封装成 Lua 层可直接 `require` 的两个 C 共享库：

- **服务器库** `kfsserverlib.so`：由 C 库驱动所有房间，处理客户端协议包，**收集后转发给同房间内所有客户端**。
- **客户端库** `kfsclientlib.so`：客户端独立库，单会话连服务器，上报输入、收服务器广播。

输入帧走 **KCP 可靠传输**；按线上帧同步标准实现：
- **输入提前量窗口**：客户端把输入补发到"当前帧 + advance_frames"，服务器从玩家超前缓冲按帧号取全员输入推进；
- **严格帧调度器**：服务器驱动线程按 frame_rate 的绝对 tick 补差推进，每 tick 每房间最多广播一帧；
- **帧对账**：服务器对广播帧全员输入算 FNV-1a32 哈希附在负载末尾，客户端重算比对，不一致即告警不同步；
- **慢玩家超时兜底**：超过阈值用最近完整帧输入填充并强制广播，避免一个玩家卡顿拖死全房。

面向商业化补齐（P0）：
- **状态对账 + 一致点**：客户端对广播帧算逻辑状态哈希经 `report_state` 上报，服务器收集全员哈希比对，推进 `sync_fid` 一致点 / 标记 `desync` 不同步；
- **会话鉴权（per-player，服务器生成）**：创建房间时传入玩家 id 表（`{[11]=true, [12]=true}`），服务器为每个玩家生成专属 token、登记到房间实例，并把 `{[conv]=token}` 映射表返回给 server 的 Lua 层；Lua 层经安全通道分发给对应客户端，客户端 `join(room_id, 自己的token)` 时按 conv 查表校验，不匹配拒收（ACK result=5）。未登记该 conv 才回退房间级 `room_token`。
- **心跳掉线判定（席位保留，可重连）**：客户端 `set_heartbeat(ms)` 自动补发 HEARTBEAT，服务器按 `last_active` 超过 `idle_timeout_ms` 把在线玩家标记掉线（offline=1，**不删除成员**：初始/普通玩家席位都保留，广播用其最近输入填充，同 conv 重连即恢复）；**准备阶段（房间未 start、尚无帧包）玩家连接全靠心跳保活**，是保留心跳的核心原因；
- **输入合法性校验**：超长输入拒绝、超前超过缓冲窗口的异常帧号拒绝（防灌帧）；
- **房间 TTL 冗余**：`new_room` 可设 `ttl_ms`，房间超过最长保留时间自动销毁，清理僵尸房间；
- **支持追帧**：客户端 `catch_up()` 丢弃积压旧广播、直接跳到服务器当前帧，供严重滞后快速恢复。

## 结构

```
KcpFrameSync/
├── kfs_src/
│   ├── common/                 共享底层（网络 / 帧 / 工具）
│   │   ├── net.c / net.h       网络层：UDP socket + 接收线程 + 原始包收发
│   │   │                       （含 net_send_to，支持多人广播）
│   │   ├── frame.c / frame.h   帧协议层：带 type 的帧编解码（CMD_INPUT/BROADCAST…）
│   │   └── kfs_common.h / kfs_util.c  公共工具（kfs_now_ms / kfs_rng_next，双库共享）
│   ├── kfsserverlib/           服务器库（底层逻辑 + Lua 绑定 分离）
│   │   ├── kfs_server.h        结构定义 + 跨模块函数声明
│   │   ├── kfs_core.c          底层逻辑：netref 句柄 / 房间收集转发 / 驱动线程 / 服务器 C 层
│   │   └── kfs_lua.c           Lua 绑定：fs.new / conn / 点对点 room / srvroom / luaopen
│   ├── kfsclientlib/           客户端库（底层逻辑 + Lua 绑定 分离）
│   │   ├── kfs_client.h        客户端对象 + 跨模块声明
│   │   ├── kfs_client_core.c   底层逻辑：KCP output 回调 + 驱动线程 + 自动心跳
│   │   └── kfs_client_lua.c    Lua 绑定：new / setpeer / join / 输入·广播·追帧 / luaopen
│   ├── third/                  第三方源码
│   │   ├── kcp/                kcp 源码（ikcp.c / ikcp.h）
│   │   └── lua-5.4.8/          Lua 5.4 C 源码（自包含编译用）
│   └── Makefile                构建 → kfsserverlib.so + kfsclientlib.so
├── lua_test/                   测试脚本（用两个 .so）
│   ├── server.lua              服务器测试脚本（多玩家房间）
│   ├── client.lua              客户端测试脚本（用客户端库）
│   ├── config                  配置（udp port / 房间 / 帧率 / conv / 客户端）
│   └── stop_all.sh             停止测试进程脚本
└── readme.md
```

## 架构：三层分离 + 双库

| 层 | 文件 | 职责 | 依赖 |
|----|------|------|------|
| 网络层（common） | `kfs_src/common/net.c` | UDP socket、接收线程、原始包队列、`net_send`/`net_send_to` | 无 |
| 帧协议层（common） | `kfs_src/common/frame.c` | 带 type 的帧编解码：`[type 1B][frame_id 4B][len 2B][data]` | 无 |
| 公共工具（common） | `kfs_src/common/kfs_util.c` | `kfs_now_ms` / `kfs_rng_next`（双库共享） | 无 |
| 服务器库 | `kfs_src/kfsserverlib/kfs_core.c` + `kfs_lua.c` | 底层逻辑（netref/房间收集/驱动/服务器C层）与 Lua 绑定分离 | Lua 5.4 |
| 客户端库 | `kfs_src/kfsclientlib/kfs_client_core.c` + `kfs_client_lua.c` | 底层逻辑（KCP output/驱动/自动心跳）与 Lua 绑定分离 | Lua 5.4 |

**核心概念升级**：旧代码里「房间 = 一个 conv = 点对点」；新架构里
**conv = 玩家会话，房间 = 多玩家集合**。一个服务器端口承载多个房间，
每个房间登记多个玩家（各一个 KCP 会话），服务器收集同房全员本帧输入后广播。

### 成员模型与玩家状态

房间内单一成员数组 `players[]`，由 `state` 区分三类占位，`ready`/`offline` 为附属标志：

| 状态 | 含义 | 参与推帧 |
|------|------|---------|
| `INIT`（初始列表） | 创建房间时预登记、尚未连接的玩家 | 不参与（等 JOIN 升级 ONLINE） |
| `ONLINE`（join 列表） | 已 JOIN 建立连接 | 参与收集/广播；掉线标记 offline 后不阻塞集齐，广播用其最近输入 |
| `LEFT`（已转房） | 玩家转房/离开后旧房间的遗留占位 | 不参与（席位保留） |

- **初始玩家**：`new_room` 传入初始玩家 id 表时，校验人数 ≤ `max_player` 后预登记为 INIT；
  客户端 JOIN 时同 conv 槽位升级为 ONLINE（首连）；
- **掉线**：超时只标记 `offline=1`，**不删除成员**（初始列表玩家掉线席位保留，可重连）；
- **转房**：玩家 JOIN 新房间时，若其在旧房间有 INIT/ONLINE 占位，先置旧房间状态为 LEFT
  （释放其 KCP、席位保留），再加入新房间——同一玩家不会同时挂两个房间；
- **就绪列表**：`ready_count()` 统计「在线且未掉线且已上报就绪」的玩家，业务层据此开战。

## 帧协议

```
type:   KFS_CMD_INPUT=1     客户端 → 服务器：上报一帧输入
        KFS_CMD_BROADCAST=2 服务器 → 客户端：广播整帧（含全员输入）
        KFS_CMD_JOIN=3      客户端 → 服务器：请求加入房间（保留）
        KFS_CMD_LEAVE=4     客户端 → 服务器：离开房间（保留）
        KFS_CMD_HEARTBEAT=5 心跳（保活 / 掉线判定）
        KFS_CMD_STATE=6     客户端 → 服务器：上报某帧逻辑状态哈希（对账）
        KFS_CMD_READY=7     客户端 → 服务器：游戏数据加载完毕，上报就绪
帧格式：[ type 1B ][ frame_id 4B ][ data_len 2B ][ data... ]
广播帧负载：逐玩家 [ conv 4B ][ len 2B ][ input... ] + 末尾 [ 对账哈希 4B ]
JOIN 握手：[ magic 4B ][ room_id 4B ][ conv 4B ][ token 4B ]（16B；12B 旧包兼容 token=0）
JOIN 应答：[ magic 4B ][ result 1B ]（0成功 1不存在 2满 3conv冲突 4其他 5token错误）
```

## 构建（Linux 服务器，自包含）

```bash
# kfs_src 目录下
cd kfs_src
make            # 产出 kfsserverlib.so + kfsclientlib.so
sudo make install   # 两个 .so 都安装到 /usr/local/lib/lua/5.4/
```

Windows + Zig 交叉编译（先设 ZIG 路径，在 kfs_src 目录下）：

```bash
make zig
```

## 服务器用法（kfsserverlib）

```lua
local fs = require("kfsserverlib")
local conn, err = fs.new(10500, 20)             -- 绑定 UDP，启动驱动线程；frame_rate=20
-- new_room(room_id, timeout_ms, token, ttl_ms, idle_timeout_ms, players)
local players = { [11] = true, [12] = true }   -- 初始玩家 id 表（key 即 conv）
local room, tokens = conn:new_room(100, 500, 0, 0, 2000, players)
--   校验：初始玩家数 ≤ max_player，否则报错；初始玩家预登记为 INIT（等 JOIN）
--   返回 tokens = {[11]=tk1, [12]=tk2}（经安全通道分发给对应客户端）
--   token:          房间级 token（0=不校验；per-player 未登记该 conv 时回退）
--   ttl_ms:         房间最长保留 ms（0=永久；超时自动销毁做冗余清理）
--   idle_timeout_ms:玩家掉线判定 ms（0=不启用；超时标记掉线，席位保留可重连）
-- 也可事后手动登记：room:set_player_token(conv, token)
-- 玩家登记：客户端发裸 UDP JOIN 后，服务器从包源地址自动学 ip:port 并
-- 登记进房间（INIT 槽位升级 ONLINE / 新 conv 追加；转房自动排重）。
-- 后台驱动线程自动：收客户端输入帧 → 集齐全员 → 广播给同房所有客户端
-- Lua 主循环只需查询/运行时维护

-- 房间状态机（准备 → 对局）：
--   phase=0 准备：等全员加载游戏数据并上报就绪，不收集不广播；
--     此阶段无帧包，玩家连接靠心跳保活，心跳超时（idle_timeout）标记掉线。
--   phase=1 对局：业务层判定全员就绪后调 room:start()，才开始收集推帧。
local players = { [11] = true, [12] = true }
local room, tokens = conn:new_room(100, 500, 0, 0, 2000, players)
while true do
    if room:get_phase() == 0 and room:ready_count() >= 2 then
        room:start()                 -- 业务层通知房间开始同步推帧
    end
    -- room:ready_count() 全员就绪计数；room:get_phase() 当前阶段
end
```

## 客户端用法（kfsclientlib）

```lua
local fs = require("kfsclientlib")
local c = fs.new(0, 11)                         -- 端口 0=系统分配；conv 唯一标识
c:setpeer("127.0.0.1", 10500)                   -- 指定服务器
local ok, why = c:join(100, 111)                -- join(room_id, 自己的token)；须与 set_player_token 一致
if not ok then error(why) end
c:nodelay(1, 10, 2, 1)
c:set_heartbeat(500)                            -- 自动心跳（供服务器掉线判定）
c:send_input(frame_id, "move:1,2")              -- 上报输入（须先 join 成功）
local bfid, payload, ok2 = c:recv_broadcast()   -- 收广播（含全员输入 + 对账哈希）
c:report_state(bfid, state_hash)                -- 上报该帧逻辑状态哈希（对账一致点）
local cfid, cdata, cok = c:catch_up()           -- 追帧：丢旧广播，跳到服务器当前帧
```

## 服务器库 API

| 方法 | 说明 |
|------|------|
| `fs.new(port, frame_rate)` | 创建连接（绑定端口，自动启动驱动线程；frame_rate 为驱动帧节拍，严格 tick 调度，默认 20） |
| `conn:new_room(room_id, timeout_ms, token, ttl_ms, idle_timeout_ms[, players])` | 创建多玩家房间；`players` 为初始玩家 id 表（校验人数 ≤ 房间容量、预登记 INIT），服务器生成 per-player token 并返回 (room, tokens) |
| `room:add_player(conv, ip, port)` | 手动登记玩家（可选；默认由客户端 JOIN 自动登记；含转房排重/INIT/掉线槽位复用） |
| `room:set_player_token(conv, token)` | 登记该玩家的专属会话 token（per-player 鉴权，JOIN 时按 conv 校验） |
| `room:remove_player(conv)` | 主动移除玩家（任何占位皆可；释放其 KCP，重置本帧收集） |
| `room:get_id()` / `room:get_count()` | 查询；get_count=成员总数（INIT+ONLINE+LEFT，掉线席位保留仍计入） |
| `room:start()` | 业务层通知房间开始同步推帧（PREPARE→PLAYING；之前不收集不广播） |
| `room:ready_count()` | 已上报就绪的玩家数（业务层据此判定全员就绪） |
| `room:get_phase()` | 房间阶段：0=准备 1=对局中 |
| `room:close()` | 关闭房间，释放全部玩家 KCP |
| `conn:create_room(conv)` | 旧点对点房间（兼容保留） |

## 客户端库 API

| 方法 | 说明 |
|------|------|
| `fs.new(port, conv)` | 创建客户端连接（port=0 系统分配 + 单 KCP 会话 + 自动驱动） |
| `c:setpeer(ip, port)` | 指定服务器地址 |
| `c:join(room_id[, token])` | 发裸 UDP JOIN 自动登记进房间（可带鉴权 token），返回 true / false+原因 |
| `c:send_input(frame_id, data)` | 上报一帧输入（打 CMD_INPUT；须先 join 成功） |
| `c:send_ready()` | 上报就绪（游戏数据加载完毕后调用；服务器据此计数判全员就绪） |
| `c:recv_broadcast()` | 收服务器广播帧，返回 frame_id, payload, 对账ok（ok=false 表示该帧哈希不一致） |
| `c:report_state(frame_id, hash)` | 上报某帧逻辑状态哈希（对账一致点） |
| `c:catch_up()` | 追帧：丢弃积压旧广播，返回服务器当前帧 (frame_id, payload, ok) |
| `c:set_heartbeat(ms)` | 设置自动心跳间隔（0=关闭；驱动线程按此补发 HEARTBEAT） |
| `c:nodelay/wndsize/setmtu` | KCP 调优 |
| `c:closed()` / `c:close()` | 查询 / 关闭 |

## 测试

# 编译生成lua5.4.8的可执行程序lua
cd kfs_src/third/lua-5.4.8 && make

```bash
# 终端1（服务器）：cd lua_test && ../kfs_src/third/lua-5.4.8/src/lua server.lua server.lua
# 终端2（客户端）：cd lua_test && ../kfs_src/third/lua-5.4.8/src/lua server.lua client.lua
```

配置文件 `config` 中的 `client_count` / `conv` 决定客户端数量与 conv；
玩家由客户端 `join()` 自动登记（端口由系统分配，不再需要 `client_base_port`）；
`advance_frames` 为输入提前量窗口（客户端把输入补发到"当前帧 + advance_frames"）。
`server.lua` 与 `client.lua` 只要保证 `room_id`、`server_port`、`conv`、`frame_rate` 一致即可。

## 开战流程（准备阶段 → 同步推帧）

1. 服务器 `new_room(...)` 创建房间：校验初始玩家数 ≤ max_player，初始玩家预登记为 INIT（phase=0，不推帧）；
2. 客户端 `join(room_id, token)` 入房（INIT 槽位升级 ONLINE）→ 加载游戏数据 → `send_ready()` 上报就绪；
3. **准备阶段**：服务器不收集不广播，玩家连接靠客户端自动心跳（`set_heartbeat`）保活，
   超过 `idle_timeout_ms` 未活跃标记掉线（席位保留、可重连）——业务层可据此解散房间或回匹配；
4. 业务层判定 `ready_count() >= 期望人数` 后调 `room:start()`，房间进入对局
   （phase=1），驱动线程才开始"集齐全员输入 → 广播"的推帧循环。

对局中掉线的玩家：标记 offline，不阻塞全员集齐（广播用其最近输入填充），
同 conv 重连（JOIN 携带原 token）即恢复。

`heartbeat_ms`（客户端）与 `idle_timeout_ms`（服务器）必须配合：
准备阶段心跳 500ms、掉线阈值 2000ms（连丢 4 个心跳判掉线）。

<img width="1920" height="1032" alt="image" src="https://github.com/user-attachments/assets/f01d6bf0-25c6-4e80-a407-d5febae4563a" />

