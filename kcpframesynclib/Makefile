#=====================================================================
# kcpframesynclib — Lua 5.4 帧同步 C 模块构建脚本（房间模型）
#
# 产物：
#   kcpframesynclib.so           （Linux 共享库，供 `require("kcpframesynclib")`）
#   build/lua/lua                （用内置 Lua 5.4.8 源码编译的解释器）
#
# 在 Linux 服务器上构建：
#   make          # 编译 kcpframesynclib.so
#   make lua      # 用内置 Lua 5.4.8 源码编译解释器 → $(BUILD_DIR)/lua/lua
#   make install  # 复制 .so 到 /usr/local/lib/lua/5.4/ 等 lua 查找路径
#
# 自包含：Lua 5.4 C 源码放在 lua/lua-5.4.8/ 下，默认用它编译，
#   无需服务器安装 liblua5.4-dev。也可用系统头文件覆盖：
#   make LUA_INC=/usr/local/include/lua5.4
#
# 说明：实现分三层 —— net.c(网络) / frame.c(帧) / kcpframesynclib.c(Lua接入)。
#       所有编译中间文件（.o）与最终产物均落在 $(BUILD_DIR)/ 下，
#       源码目录保持整洁。BUILD_DIR 可用命令行覆盖：
#       make BUILD_DIR=/tmp/kfsbuild
#=====================================================================

# ---- 输出目录（可覆盖） ----------------------------------------------
BUILD_DIR ?= build

# ---- 编译器与 Lua 头文件路径 ----------------------------------------
CC      ?= gcc
# Lua 头文件目录：默认指向本地源码；可用 make LUA_INC=... 覆盖
LUA_INC ?= -Ilua/lua-5.4.8/src

CFLAGS  += -O2 -Wall -Wextra -fPIC -fvisibility=hidden $(LUA_INC) -I.
LIBS     = -lpthread
SOFLAGS  = -shared

# 服务器库与客户端库两个产物（共享 ikcp/net/frame 底层）
TARGET_SERVER = $(BUILD_DIR)/kcpframesynclib.so
TARGET_CLIENT = $(BUILD_DIR)/kcpframesynclib_client.so
OBJS_COMMON = $(BUILD_DIR)/kcp/ikcp.o \
              $(BUILD_DIR)/net.o \
              $(BUILD_DIR)/frame.o
OBJS_SERVER = $(OBJS_COMMON) $(BUILD_DIR)/kcpframesynclib.o
OBJS_CLIENT = $(OBJS_COMMON) $(BUILD_DIR)/kcpframesynclib_client.o

# ---- 目标规则 --------------------------------------------------------
all: $(TARGET_SERVER) $(TARGET_CLIENT)

$(TARGET_SERVER): $(OBJS_SERVER)
	$(CC) $(SOFLAGS) -o $@ $(OBJS_SERVER) $(LIBS)

$(TARGET_CLIENT): $(OBJS_CLIENT)
	$(CC) $(SOFLAGS) -o $@ $(OBJS_CLIENT) $(LIBS)

$(BUILD_DIR)/kcp/ikcp.o: kcp/ikcp.c kcp/ikcp.h
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c -o $@ kcp/ikcp.c

$(BUILD_DIR)/net.o: kcpframesynclib/net.c kcpframesynclib/net.h
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c -o $@ kcpframesynclib/net.c

$(BUILD_DIR)/frame.o: kcpframesynclib/frame.c kcpframesynclib/frame.h
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c -o $@ kcpframesynclib/frame.c

$(BUILD_DIR)/kcpframesynclib.o: kcpframesynclib/kcpframesynclib.c kcp/ikcp.h kcpframesynclib/net.h kcpframesynclib/frame.h
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c -o $@ kcpframesynclib/kcpframesynclib.c

$(BUILD_DIR)/kcpframesynclib_client.o: kcpframesynclib/kcpframesynclib_client.c kcp/ikcp.h kcpframesynclib/net.h kcpframesynclib/frame.h
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c -o $@ kcpframesynclib/kcpframesynclib_client.c

# 安装到系统 Lua 模块路径（Ubuntu/Debian 默认）
PREFIX ?= /usr/local
LUA_VERSION ?= 5.4
install: $(TARGET_SERVER) $(TARGET_CLIENT)
	install -d $(DESTDIR)$(PREFIX)/lib/lua/$(LUA_VERSION)
	install -m 0755 $(TARGET_SERVER) $(DESTDIR)$(PREFIX)/lib/lua/$(LUA_VERSION)/
	install -m 0755 $(TARGET_CLIENT) $(DESTDIR)$(PREFIX)/lib/lua/$(LUA_VERSION)/

# ---- 自包含 Lua 5.4.8 解释器（中间文件在 $(BUILD_DIR)/lua/） ----------
# 用法：make lua  → 产出 $(BUILD_DIR)/lua/lua 可执行文件
# 与官方 linux 目标同 flags：-DLUA_USE_LINUX、-Wl,-E -ldl、-std=gnu99
LUA_SRC  = lua/lua-5.4.8/src
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
		$(LUA_INC) -I. \
		-shared -o $(TARGET_SERVER) kcp/ikcp.c kcpframesynclib/net.c kcpframesynclib/frame.c kcpframesynclib/kcpframesynclib.c -lpthread
	$(ZIG) cc -target $(ZIG_TARGET) -O2 -fPIC -fvisibility=hidden \
		$(LUA_INC) -I. \
		-shared -o $(TARGET_CLIENT) kcp/ikcp.c kcpframesynclib/net.c kcpframesynclib/frame.c kcpframesynclib/kcpframesynclib_client.c -lpthread

clean:
	rm -rf $(BUILD_DIR)

.PHONY: all install zig lua clean
