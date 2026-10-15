#!/usr/bin/env bash


# 可执行权限
# chmod +x start_server.sh
# 启动测试脚本 参数1是房间数 参数2是每个房间人数
# ./start_server.sh 5 3

#删除tokens相关文件
rm -rf ./tokens_*

../kfs_src/third/lua-5.4.8/src/lua server.lua $1 $2