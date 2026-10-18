## 帧同步网页demo
帧同步的客户端示例。


## 如何运行
* 服务端
```shell
$ cd gateway
$ npm install
$ node gateway.js
```

* 如果udp帧同步服务器是运行在docker,windows宿主机的直接发到docker的udp端口会丢包
* 客户端跑在宿主机，我在windows宿主机写一个 TCP -> UDP 桥（node bridge_windows.js）
```shell
$ node bridge_windows.js
```
显示：
[bridge] windows bridge udp 127.0.0.1:10501 -> tcp 127.0.0.1:10500
[bridge] Unity kfs 客户端 setpeer("127.0.0.1", 10501)

* 还有一种方法比如我的网页demo的网关也部署到docker路面（start_container.sh）



* 客户端（也可使用任意方法启动一个web服务）
```shell
$ cd client
$ python3 -m http.server 8080
```

* 启动完成后访问：[http://127.0.0.1:8080/](http://127.0.0.1:8080/)

## 示例截图
![](README/preview.png)