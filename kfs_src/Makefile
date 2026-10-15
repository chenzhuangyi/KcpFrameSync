#=====================================================================
# KcpFrameSync — Lua 5.4 帧同步 C 模块构建脚本（kfsserverlib/kfsclientlib/common 结构）
#
# 目录结构（kfs_src/）：
#   common/       net.c frame.c kfs_util.c（共享网络/帧/工具）
#   kfsserverlib/ kfs_server.h kfs_core.c（底层） kfs_lua.c（Lua 绑定）
#   kfsclientlib/ kfs_client.h kfs_client_core.c（底层） kfs_client_lua.c（Lua 绑定）
#   third/        kcp/（ikcp.c/ikcp.h） + lua-5.4.8/（Lua 5.4 C 源码）
#
# 产物：
#   build/kfsserverlib.so      （服务器库，require("kfsserverlib")）
#   build/kfsclientlib.so      （客户端库，require("kfsclientlib")）
#   build/lua/lua              （用内置 Lua 5.4.8 源码编译的解释器）
#
# 在 Linux 服务器上构建：
#   make          # 编译两个 .so
#   make lua      # 用内置 Lua 5.4.8 源码编译解释器 → $(BUILD_DIR)/lua/lua
#   make install  # 复制 .so 到 /usr/local/lib/lua/5.4/ 等 lua 查找路径
#
# 自包含：Lua 5.4 C 源码放在 third/lua-5.4.8/ 下，默认用它编译，
#   无需服务器安装 liblua5.4-dev。也可用系统头文件覆盖：
#   make LUA_INC=/usr/local/include/lua5.4
#
# 说明：实现分三层 —— net.c/frame.c（common 网络帧）/ kfs_core.c + kfs_lua.c
#       （服务器）/ kfs_client_core.c + kfs_client_lua.c（客户端）。
#       所有编译中间文件（.o）与最终产物均落在 $(BUILD_DIR)/ 下，
#       源码目录保持整洁。BUILD_DIR 可用命令行覆盖：
#       make BUILD_DIR=/tmp/kfsbuild
#=====================================================================

# ---- 输出目录（可覆盖） ----------------------------------------------
BUILD_DIR ?= build

# ---- 编译器与 Lua 头文件路径 ----------------------------------------
CC      ?= gcc
# Lua 头文件目录：默认指向本地源码；可用 make LUA_INC=... 覆盖
LUA_INC ?= -Ithird/lua-5.4.8/src

# include 路径：
#   -Icommon  提供 net.h/frame.h/kfs_common.h
#   -Ithird   提供 kcp/ikcp.h（third/kcp/ikcp.h）
CFLAGS  += -O2 -Wall -Wextra -fPIC -fvisibility=hidden $(LUA_INC) -Icommon -Ithird
LIBS     = -lpthread
SOFLAGS  = -shared

# 服务器库与客户端库两个产物（共享 common 底层 + third/kcp）
TARGET_SERVER = $(BUILD_DIR)/kfsserverlib.so
TARGET_CLIENT = $(BUILD_DIR)/kfsclientlib.so
OBJS_COMMON = $(BUILD_DIR)/kcp/ikcp.o \
              $(BUILD_DIR)/net.o \
              $(BUILD_DIR)/frame.o \
              $(BUILD_DIR)/kfs_util.o
OBJS_SERVER = $(OBJS_COMMON) \
              $(BUILD_DIR)/kfsserverlib/kfs_core.o \
              $(BUILD_DIR)/kfsserverlib/kfs_lua.o
OBJS_CLIENT = $(OBJS_COMMON) \
              $(BUILD_DIR)/kfsclientlib/kfs_client_core.o \
              $(BUILD_DIR)/kfsclientlib/kfs_client_lua.o

# ---- 目标规则 --------------------------------------------------------
all: $(TARGET_SERVER) $(TARGET_CLIENT)

$(TARGET_SERVER): $(OBJS_SERVER)
	$(CC) $(SOFLAGS) -o $@ $(OBJS_SERVER) $(LIBS)

$(TARGET_CLIENT): $(OBJS_CLIENT)
	$(CC) $(SOFLAGS) -o $@ $(OBJS_CLIENT) $(LIBS)

$(BUILD_DIR)/kcp/ikcp.o: third/kcp/ikcp.c third/kcp/ikcp.h
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c -o $@ third/kcp/ikcp.c

$(BUILD_DIR)/net.o: common/net.c common/net.h
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c -o $@ common/net.c

$(BUILD_DIR)/frame.o: common/frame.c common/frame.h
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c -o $@ common/frame.c

$(BUILD_DIR)/kfs_util.o: common/kfs_util.c common/kfs_common.h
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c -o $@ common/kfs_util.c

$(BUILD_DIR)/kfsserverlib/kfs_core.o: kfsserverlib/kfs_core.c kfsserverlib/kfs_server.h
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c -o $@ kfsserverlib/kfs_core.c

$(BUILD_DIR)/kfsserverlib/kfs_lua.o: kfsserverlib/kfs_lua.c kfsserverlib/kfs_server.h
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c -o $@ kfsserverlib/kfs_lua.c

$(BUILD_DIR)/kfsclientlib/kfs_client_core.o: kfsclientlib/kfs_client_core.c kfsclientlib/kfs_client.h
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c -o $@ kfsclientlib/kfs_client_core.c

$(BUILD_DIR)/kfsclientlib/kfs_client_lua.o: kfsclientlib/kfs_client_lua.c kfsclientlib/kfs_client.h
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c -o $@ kfsclientlib/kfs_client_lua.c

# 安装到系统 Lua 模块路径（Ubuntu/Debian 默认）
PREFIX ?= /usr/local
LUA_VERSION ?= 5.4
install: $(TARGET_SERVER) $(TARGET_CLIENT)
	install -d $(DESTDIR)$(PREFIX)/lib/lua/$(LUA_VERSION)
	install -m 0755 $(TARGET_SERVER) $(DESTDIR)$(PREFIX)/lib/lua/$(LUA_VERSION)/
	install -m 0755 $(TARGET_CLIENT) $(DESTDIR)$(PREFIX)/lib/lua/$(LUA_VERSION)/

# ---- 自包含 Lua 5.4.8 解释器（中间文件在 $(BUILD_DIR)/lua/） ----------
# 用法：make lua  → 产出 $(BUILD_DIR)/lua/lua 可执行文件
LUA_SRC  = third/lua-5.4.8/src
LUA_CORE = lapi lcode lctype ldebug ldo ldump lfunc lgc llex lmem \
           lobject lopcodes lparser lstate lstring ltable ltm lundump lvm lzio
LUA_LIB  = lauxlib lbaselib lcorolib ldblib liolib lmathlib \
           loadlib loslib lstrlib ltablib lutf8lib linit
LUA_OBJS = $(addprefix $(BUILD_DIR)/lua/,$(addsuffix .o,$(LUA_CORE) $(LUA_LIB) lua))

lua: $(BUILD_DIR)/lua/lua

$(BUILD_DIR)/lua/lua: $(LUA_OBJS)
	$(CC) -o $@ -Wl,-E -ldl $(LUA_OBJS) -lm

$(BUILD_DIR)/lua/%.o: $(LUA_SRC)/%.c
	@mkdir -p $(dir $@)
	$(CC) -std=gnu99 -O2 -Wall -Wextra -DLUA_COMPAT_5_3 -DLUA_USE_LINUX \
		-I$(LUA_SRC) -c -o $@ $<

# ---- 交叉编译：Windows + Zig 直接产出 Linux .so -----------------------
# 用法：先设置 ZIG（zig 可执行文件路径），再 make zig
ZIG     ?= zig
ZIG_TARGET ?= x86_64-linux-gnu
zig:
	$(ZIG) cc -target $(ZIG_TARGET) -O2 -fPIC -fvisibility=hidden \
		$(LUA_INC) -Icommon -Ithird \
		-shared -o $(TARGET_SERVER) \
		third/kcp/ikcp.c common/net.c common/frame.c common/kfs_util.c \
		kfsserverlib/kfs_core.c kfsserverlib/kfs_lua.c -lpthread
	$(ZIG) cc -target $(ZIG_TARGET) -O2 -fPIC -fvisibility=hidden \
		$(LUA_INC) -Icommon -Ithird \
		-shared -o $(TARGET_CLIENT) \
		third/kcp/ikcp.c common/net.c common/frame.c common/kfs_util.c \
		kfsclientlib/kfs_client_core.c kfsclientlib/kfs_client_lua.c -lpthread

clean:
	rm -rf $(BUILD_DIR)

.PHONY: all install zig lua clean
