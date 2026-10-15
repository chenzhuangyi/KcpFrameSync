#!/usr/bin/env bash


#可执行权限
# chmod +x ./start_client.sh
#启动测试脚本 参数1是房间id
# ./start_client.sh 1

#echo "第一个参数: $1"
../kfs_src/third/lua-5.4.8/src/lua client.lua $1