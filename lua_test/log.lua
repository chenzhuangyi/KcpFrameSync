local LOG_LEVEL_DEBUG = "DEBUG"
local LOG_LEVEL_INFO  = "INFO"
local LOG_LEVEL_WARN  = "WARN"
local LOG_LEVEL_ERROR = "ERROR"
local LOG_LEVEL_NONE  = "NONE"

-- 当前最低输出级别，改成 LOG_LEVEL_INFO 就不会输出 DEBUG
local current_level = LOG_LEVEL_DEBUG

local LEVEL_ORDER = {
    [LOG_LEVEL_DEBUG] = 1,
    [LOG_LEVEL_INFO]  = 2,
    [LOG_LEVEL_WARN]  = 3,
    [LOG_LEVEL_ERROR] = 4,
    [LOG_LEVEL_NONE]  = 5,
}

local function should_log(level)
    return (LEVEL_ORDER[level] or 0) >= (LEVEL_ORDER[current_level] or 0)
end

local function get_short_source(stack_level)
    stack_level = stack_level or 4
    local info = debug.getinfo(stack_level, "Sl")
    if not info then
        return "lua", 0
    end

    local source = info.short_src or "lua"
    local line = info.currentline or 0

    local idx = source:find("[^/\\]+$")
    if idx then
        source = source:sub(idx)
    end

    return source, line
end

local function logf(level, stack_level, fmt, ...)
    if not should_log(level) then
        return
    end

    local source, line = get_short_source(stack_level)
    local ts = os.date("%Y-%m-%d %H:%M:%S")
    local msg = string.format(fmt, ...)

    print(string.format("[%s][%s][lua][%s:%d] %s", ts, level, source, line, msg))
end

function LOG_DEBUGF(fmt, ...)
    logf(LOG_LEVEL_DEBUG, 4, fmt, ...)
end

function LOG_INFOF(fmt, ...)
    logf(LOG_LEVEL_INFO, 4, fmt, ...)
end

function LOG_WARNF(fmt, ...)
    logf(LOG_LEVEL_WARN, 4, fmt, ...)
end

function LOG_ERRORF(fmt, ...)
    logf(LOG_LEVEL_ERROR, 4, fmt, ...)
end

-- 如果不希望格式化，直接打印字符串，可以用这个
function LOG_DEBUG(msg)
    logf(LOG_LEVEL_DEBUG, 4, "%s", tostring(msg))
end

function LOG_INFO(msg)
    logf(LOG_LEVEL_INFO, 4, "%s", tostring(msg))
end

function LOG_WARN(msg)
    logf(LOG_LEVEL_WARN, 4, "%s", tostring(msg))
end

function LOG_ERROR(msg)
    logf(LOG_LEVEL_ERROR, 4, "%s", tostring(msg))
end


LOG_DEBUGF("lua 日志模块初始化完成")