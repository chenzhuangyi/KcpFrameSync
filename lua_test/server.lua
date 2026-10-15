--=====================================================================
-- server.lua — KCP 帧同步库 服务器测试脚本（多玩家房间收集转发）
--
-- 功能：
--   1. 读取 lua_test/config 的服务器与房间参数
--   2. 用 kfsserverlib 绑定 UDP 端口，创建多玩家房间 new_room
--   3. 玩家由客户端发裸 UDP JOIN 自动登记（服务器从包源地址学地址），
--      无需预先 add_player(conv, ip, port)
--   4. 后台驱动线程自动：收客户端输入帧 → 集齐全员 → 广播给同房所有客户端
--   5. Lua 主循环只负责运行时维护与状态打印（驱动已由 C 库接管）
--
-- 依赖：Lua 5.4，无第三方 socket 库（收发包/驱动由 C 库完成）
--=====================================================================

package.cpath = package.cpath .. ";../kfs_src/build/?.so"
local kfsserverlib = require("kfsserverlib")

-----------------------------------------------------------------------
-- 读取 lua_test/config 配置文件（轻量 key=value，# 开头为注释）
-----------------------------------------------------------------------
local function load_config(path)
    local f = assert(io.open(path, "r"), "无法打开配置文件: " .. path)
    local cfg = {}
    for line in f:lines() do
        line = line:gsub("^%s+", ""):gsub("%s+$", "")
        if line ~= "" and line:sub(1, 1) ~= "#" then
            local key, val = line:match("^([%w_]+)%s*=%s*(.*)$")
            if key then
                val = val:gsub('^"(.*)"$', "%1"):gsub("^'(.*)'$", "%1")
                cfg[key] = tonumber(val) or val
            end
        end
    end
    f:close()
    return cfg
end

local CONFIG_PATH = "config"   -- 运行目录下；如不在，可改为绝对路径
local cfg = load_config(CONFIG_PATH)

local udp_port      = cfg.udp_port      or 10500
local max_player    = cfg.max_player    or 4
local timeout_ms    = cfg.timeout_ms    or 500
local frame_rate    = cfg.frame_rate    or 20
local duration_ms   = cfg.duration_ms   or 5000
local room_token    = cfg.token         or 0   -- 会话鉴权 token（0=不校验）
local room_ttl      = cfg.ttl_ms        or 0   -- 房间最长保留 ms（0=永久）
local idle_timeout  = cfg.idle_timeout_ms or 0 -- 玩家掉线判定 ms（0=不启用）

-- 线上业务可以考虑全服唯一id
local room_id       = cfg.room_id       or 100

print(string.format("[server lua] 配置: udp=%s 房间=%d 最大玩家=%d 超时=%dms 帧率=%d 时长=%dms token=%d ttl=%dms idle=%dms",
      tostring(udp_port), room_id, max_player, timeout_ms, frame_rate, duration_ms,
      room_token, room_ttl, idle_timeout))

-----------------------------------------------------------------------
-- 绑定 UDP 端口，创建多玩家房间
-----------------------------------------------------------------------
local conn, err = kfsserverlib.new(udp_port, frame_rate)
if not conn then
    print("[server lua] 绑定端口失败: " .. tostring(err))
    os.exit(1)
end

print(string.format("[server lua] 已绑定 UDP %s，frame_rate=%d，可以创建房间了",
      tostring(udp_port), frame_rate))

-- 玩家 id 表：服务器创建房间时为其生成专属 token（per-player 鉴权）
local function parse_id_list(s)
    local t = {}
    -- 玩家数量
    local player_count = 0
    for v in (s or ""):gmatch("[^,]+") do
        local id = tonumber(v)
        if id then 
            t[id] = true 
            player_count = player_count + 1
        end
    end
    return t, player_count
end

local arg = {...}
local room_count = arg[1]
if not room_count then
    print(string.format("[client][lua] 参数1为nil"))
    return
end
room_count = tonumber(room_count)
if room_count <= 0 or room_count > 100 then
    print(string.format("[client][lua] 参数 room_count=%d", room_count))
end

local room_player_count = arg[2]
if not room_player_count then
    print(string.format("[client][lua] 参数2为nil"))
    return
end
room_player_count = tonumber(room_player_count)
if room_player_count <= 0 then
    print(string.format("[client][lua] 参数 room_player_count=%d", room_player_count))
end

-- new_room 失败会抛 Lua 错误，故用 pcall 捕获
-- 参数：room_id, timeout_ms, token, ttl_ms, idle_timeout_ms, room_players
-- 房间容量 = 初始玩家列表长度（C 库已校验初始人数 ≤ 8）
-- 传 room_players 时返回 (room, tokens)；tokens = {[conv]=服务器生成的专属token}
local rooms = {}
for i = 1, room_count do
    local room_id = i
    local room_players = {}
    for ii = 1, room_player_count do
        room_players[room_id*100 + ii] = true
    end
    local ok, room, tokens = pcall(function()
        return conn:new_room(room_id, timeout_ms,
                            room_token, room_ttl, idle_timeout, room_players)
    end)
    if not ok then
        print("[server lua] 创建房间失败: " .. tostring(room))
        conn:close()
        os.exit(1)
    end
    rooms[room_id] = room
    print(string.format("[server lua] 创建房间 room_id=%d，等待客户端 join() 自动登记", room_id))

    -----------------------------------------------------------------------
    -- 服务器生成的 per-player token：打印并写入 tokens_xxx.txt（模拟分发通道）。
    --   真实部署中应经安全通道发给对应玩家（登录服/房间服下发）。
    -----------------------------------------------------------------------
    if tokens and next(tokens) then
        local tokens_file_name = string.format("tokens_%d.txt",room_id)
        local f = assert(io.open(tokens_file_name, "w"))
        for conv, tk in pairs(tokens) do
            f:write(string.format("%d %d\n", conv, tk))
            print(string.format("[server lua] 房间 room_id=%d 玩家 conv=%d 专属token=%d", room_id, conv, tk))
        end
        f:close()
        print(string.format("[server lua] 房间 room_id=%d，写入 %s 供客户端模拟领取", room_id, tokens_file_name))
    end
end

-----------------------------------------------------------------------
-- 主循环：驱动线程已自动收包/收集/广播；Lua 只维护运行时并打印
--   room:get_count() 反映客户端 JOIN 后自动登记的人数
-----------------------------------------------------------------------
local start_time = os.clock() * 1000
local last_print = start_time

while true do
    local now_ms = math.floor(os.clock() * 1000)

    if now_ms - start_time >= duration_ms then
        break
    end

    -- 业务层判活：全员连接并上报就绪后，通知房间开始同步推帧。
    -- 准备阶段（phase=0）没有帧包，玩家连接全靠心跳（heartbeat_ms）保活，
    -- 心跳超时（idle_timeout_ms）标记掉线——这就是准备阶段必须保留心跳的原因。
    for i, room in pairs(rooms) do
        if room:get_phase() == 0 then
            local ready = room:ready_count()
            local expect_players = room:init_count()
            if ready >= expect_players then
                room:start()
                print(string.format("[server lua] 全员就绪(%d/%d)，房间开始同步推帧",
                      ready, expect_players))
            end
        end
    
        -- 每秒打印一次房间状态
        if now_ms - last_print >= 500 then
            last_print = now_ms
            print(string.format("[server lua][%d] 运行 %.1fs  phase=%d 房间=%d 玩家=%d 就绪=%d",
                os.time(), (now_ms - start_time) / 1000, room:get_phase(),
                room:get_id(), room:get_count(), room:ready_count()))
                
            if room:get_phase() == 0 then
                -- 超时
                room:start()
                local ready = room:ready_count()
                local expect_players = room:init_count()
                print(string.format("[server lua] 超时开始同步推帧 就绪(%d/%d)",
                      ready, expect_players))
            end
        end
    end

    os.execute("sleep 0.01")
end

-----------------------------------------------------------------------
-- 关闭房间与连接
-----------------------------------------------------------------------
for k, room in pairs(rooms) do
    room:close()
end
conn:close()
print("[server lua] 房间与连接已关闭")
