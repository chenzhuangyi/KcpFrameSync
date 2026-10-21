--=====================================================================
-- client.lua — KCP 帧同步库 客户端测试脚本
--
-- 功能：
--   1. 读取 lua_test/config 的服务器地址/房间参数
--   2. 每个客户端用 kfsclientlib 创建会话，端口传 0（系统分配）
--   3. c:setpeer(server_ip, server_port) 指定服务器
--   4. c:join(room_id) 发裸 UDP JOIN，服务器从源地址自动学本客户端地址
--      并登记进房间；收到 ACK 后 join() 返回 true
--   5. 输入提前量 + 锁步：收到服务器广播后补发输入到"当前帧+advance_frames"，
--      服务器从玩家超前缓冲按帧号取全员输入推进（标准帧同步输入窗口）。
--   6. 对账：recv_broadcast 第三返回值 ok=false 表示该帧全员输入哈希与
--      服务器不一致（不同步告警）；另对广播帧算确定性哈希经 report_state
--      上报，服务器据此推进"状态一致点"或标记不同步。
--   7. 心跳/追帧：set_heartbeat(ms) 让客户端自动补发 HEARTBEAT 保活；
--      catch_up() 可丢弃积压旧广播、直接跳到服务器当前帧（严重滞后时）。
--
-- 依赖：Lua 5.4，无第三方 socket 库
--=====================================================================

require "log"

package.cpath = package.cpath .. ";../kfs_src/build/?.so"
local kfs = require("kfsclientlib")

-----------------------------------------------------------------------
-- 读取 lua_test/config
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

local cfg = load_config("config")

local server_ip   = cfg.server_ip     or "127.0.0.1"
local server_port = cfg.server_port   or 10500
local frame_rate  = cfg.frame_rate    or 20
local duration_ms = cfg.clinten_duration_ms   or 5000
local advance_frames = cfg.advance_frames or 2   -- 输入提前量窗口（帧）
local heartbeat_ms   = cfg.heartbeat_ms  or 0   -- 自动心跳间隔 ms（0=关闭）

local client_count = 0
local arg = {...}
local room_id = arg[1]
if not room_id then
    LOG_DEBUGF(string.format("[client][lua] 参数为nil"))
    return
end
room_id = tonumber(room_id)
if room_id <= 0 then
    LOG_DEBUGF(string.format("[client][lua] 参数 room_id=%d", room_id))
end

-- 服务器生成的 per-player token（server.lua 写入 tokens.txt，模拟分发通道）：
-- 客户端按自己的 conv 领取 token，凭它 join 房间。
local function load_server_tokens()
    local t = {}
    local tokens_file_name = string.format("tokens_%d.txt",room_id)
    local f = io.open(tokens_file_name, "r")
    if not f then return t end
    for line in f:lines() do
        local conv, tk = line:match("^(%d+)%s+(%d+)")
        if conv then 
            t[tonumber(conv)] = tonumber(tk) 
            client_count = client_count + 1
        end
    end
    f:close()
    return t
end
local server_tokens = load_server_tokens()

-----------------------------------------------------------------------
-- 解析广播负载：逐玩家 [conv 4B][len 2B][data]；末尾 4B 为服务器对账哈希
--   （由客户端库 recv_broadcast 校验，ok=false 即不一致），解析时跳过。
-----------------------------------------------------------------------
local function parse_broadcast(payload)
    local players = {}
    local body_len = #payload - 4      -- 末尾 4B 为对账哈希
    local off = 1
    if body_len < 6 then return players end
    while off + 6 <= body_len + 1 do
        local conv = string.unpack("I4", payload, off); off = off + 4
        local len  = string.unpack("I2", payload, off); off = off + 2
        if off + len > body_len + 1 then break end
        local data = payload:sub(off, off + len - 1)
        off = off + len
        players[#players + 1] = { conv = conv, data = data }
    end
    return players
end

-----------------------------------------------------------------------
-- 构造一个客户端：不绑固定端口(0=系统分配)，setpeer 后 join 自动登记
-----------------------------------------------------------------------
local function make_client(conv)
    local c, err = kfs.new(0, conv)         -- 端口 0：系统自动分配
    if not c then
        return nil, "new失败: " .. tostring(err)
    end
    c:setpeer(server_ip, server_port)
    c:nodelay(1, 10, 2, 1)
    if heartbeat_ms > 0 then c:set_heartbeat(heartbeat_ms) end   -- 自动心跳保活

    -- 发 JOIN（带该玩家自己的 token），等待服务器 ACK（内部短超时）
    -- token 来源：config 手动指定 → 服务器下发的 tokens.txt → 房间级 token
    local own_token = server_tokens[conv]
    local ok, why = c:join(room_id, own_token)
    if not ok then
        c:close()
        return nil, "join失败: " .. tostring(why)
    end
    -- 模拟"游戏数据加载完毕"：上报就绪。服务器据此判定全员就绪后，
    -- 由 server.lua 调 room:start() 开始同步推帧（此前后端只靠心跳保活）。
    c:send_ready()
    return c, conv
end

LOG_DEBUGF(string.format("[client] 服务器 %s:%d 房间 %d 客户端数=%d 帧率=%d 时长=%dms",
      server_ip, tostring(server_port), room_id, client_count,
      frame_rate, duration_ms))

-----------------------------------------------------------------------
-- 每个客户端一个进程场景：此处模拟“依次建立并各自上报”的调度
--   单进程多客户端仅用于演示协议，真正运行时建议每个客户端独立进程。
-----------------------------------------------------------------------
local clients = {}
local convs   = {}
local startup = os.clock() * 1000

local join_player = 0
for conv, token in pairs(server_tokens) do
    local c, err = make_client(conv)
    if not c then
        LOG_DEBUGF(string.format("[client] conv=%d 加入失败: %s", conv, tostring(err)))
    else
        join_player = join_player + 1
        clients[join_player] = c
        convs[join_player] = conv
        LOG_DEBUGF(string.format("[client%d] 已登记 conv=%d 并上报就绪", join_player, conv))
    end
    os.execute("sleep 0.02")     -- 错开启动，便于观察服务器逐人登记
end


if #clients == 0 then
    LOG_DEBUGF("[client] 无客户端可用，退出")
    os.exit(1)
end

-----------------------------------------------------------------------
-- 上报/收广播主循环（输入提前量 + 锁步 + 对账）
--   输入提前量窗口：客户端把输入补发到"当前裁决帧 + advance_frames"，
--   服务器从玩家超前缓冲按帧号取全员输入推进，避免每帧现场等最慢玩家。
--   锁步：所有客户端都收到服务器广播 min_bfid 后，裁决帧号才推进到
--   min_bfid+1 并补发输入，保证全员上报帧号对齐。
--   对账：recv_broadcast 第三返回值 ok=false 表示该帧全员输入哈希不一致。
-----------------------------------------------------------------------
local next_fid = 0      -- 客户端裁决帧号（服务器已广播到的最小公共帧）
local send_fid = -1     -- 已发出的最高目标帧
local start_time = os.clock() * 1000

-- 把输入补发到 next_fid + advance_frames（全员一起发，帧号一致）
local function topup()
    local target_max = next_fid + advance_frames
    -- LOG_DEBUGF("send_fid=%d, target_max=%d", send_fid, target_max)
    while send_fid < target_max do
        send_fid = send_fid + 1
        -- LOG_DEBUGF("send_fid=%d, target_max=%d", send_fid, target_max)
        if send_fid >= 0 then
            -- LOG_DEBUGF("send_fid=%d, target_max=%d, #clients=%d", send_fid, target_max, #clients)
            for i = 1, #clients do
                LOG_DEBUGF("conv=%d, send_fid=%d, target_max=%d, #clients=%d", convs[i], send_fid, target_max, #clients)
                clients[i]:send_input(send_fid,
                                      string.format("p%d:f%d", convs[i], send_fid))
            end
        end
    end
end
topup()    -- 初始把输入补到 advance_frames

while true do
    local now_ms = math.floor(os.clock() * 1000)
    if now_ms - start_time >= duration_ms then
        break
    end

    -- 本轮：从每个客户端取一帧广播，求所有客户端都收到的最小帧号
    local min_bfid = nil
    local all_rcv  = true
    for i = 1, #clients do
        local bfid, payload, ok = clients[i]:recv_broadcast()
        if bfid then
            if not ok then
                LOG_DEBUGF(string.format("[client%d] 对账失败：fid=%d 哈希不一致（不同步）",
                      i, bfid))
            end
            local players = parse_broadcast(payload)
            local brief = {}
            for _, p in ipairs(players) do
                brief[#brief + 1] = "c" .. p.conv .. ":" .. p.data
            end
            LOG_DEBUGF(string.format("[client%d][%d] 收到广播 fid=%d 玩家数=%d [%s]",
                  i, os.time(), bfid, #players, table.concat(brief, " ")))
            -- P0-1 状态对账：对广播帧负载算确定性哈希上报，服务器收集全员
            -- 哈希推进一致点/标记不同步（同一帧全员负载相同 → 哈希一致）。
            local h = 0
            for k = 1, #payload do
                h = (h * 31 + payload:byte(k)) % 4294967296
            end
            clients[i]:report_state(bfid, h)
            if (not min_bfid) or bfid < min_bfid then
                min_bfid = bfid
            end
        else
            all_rcv = false            -- 该客户端还没有本帧广播
        end
    end

    -- 所有客户端都已收到广播 min_bfid → 裁决帧号推进并补发下一批输入
    if all_rcv and min_bfid then
        local target = min_bfid + 1
        if target > next_fid then
            next_fid = target
            topup()
            LOG_DEBUGF(string.format("[client] 全员收到 fid=%d，补发输入到 fid=%d",
                  min_bfid, send_fid))
        end
    end
    if min_bfid == 10 then
        os.execute("sleep 5")
    end

    os.execute("sleep 0.02")     -- 轮询间隔；实际帧节奏由服务器 fps 决定
end

-----------------------------------------------------------------------
-- 关闭全部客户端
-----------------------------------------------------------------------
for i = 1, #clients do
    clients[i]:close()
end
LOG_DEBUGF("[client] 全部客户端已关闭")
