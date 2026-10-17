// ============================================================
// 帧同步逻辑验证 Demo —— WebSocket 真客户端（经网关连 Docker kfs 服务器）
//
// 链路：浏览器(WebSocket) ⇄ gateway.js(本机 Node) ⇄ UDP/KCP ⇄ Docker kfs 服务器
//
// 协议对齐 kfs 库真实客户端（kfs_client_core.c / client.lua）：
//   1. 登录填 room_id / conv / token → 网关代发裸 UDP JOIN 握手（服务器校验
//      room_id + conv + token，回 ACK result）
//   2. joined 成功后 send_ready()（KCP 内 READY 帧，等待 server.lua room:start）
//   3. 输入提前量：topup() 把输入补发到 next_fid + advance_frames(=2)
//      （服务器从玩家超前缓冲按帧号取全员输入推进，标准帧同步输入窗口）
//   4. 收到服务器广播 → 解析逐玩家 [conv 4B][len 2B][data] + 尾 4B 对账哈希
//      → 本地确定性状态推进 → 上报 STATE 哈希（h=(h*31+byte)%2^32，对齐
//      client.lua 的 report_state）
//   5. 心跳由网关按 heartbeat_ms 自动补发 HEARTBEAT 帧（对齐 set_heartbeat）
//   6. 锁步：所有客户端都收到广播 min_bfid 后，裁决帧号才推进到 min_bfid+1
//      并 topup，保证全员上报帧号对齐
// ============================================================

var DIRECTION = { STOP:0, UP:1, DOWN:2, LEFT:3, RIGHT:4 };
var DIR_KEYS   = { 38:DIRECTION.UP, 40:DIRECTION.DOWN, 37:DIRECTION.LEFT, 39:DIRECTION.RIGHT, 13:DIRECTION.STOP };

var WIDTH=600, HEIGHT=420, BOX_SIZE=26;
var COLORS=["#e74c3c","#3498db","#27ae60","#9b59b6","#f39c12","#1abc9c","#e67e22","#2c3e50"];

// ---------- hex 工具（网关二进制字段用 hex 传输）----------
function hexToBytes(hex){
	var out=new Uint8Array(hex.length/2);
	for(var i=0;i<out.length;i++){ out[i]=parseInt(hex.substr(i*2,2),16); }
	return out;
}

// ---------- 确定性游戏对象 ----------
function GameObject(conv){
	this.conv=conv;
	// 所有玩家统一起点 x，按 conv 分行排布：
	// 旧公式按 conv 错开 x，靠近右侧墙的玩家会更早停下，看起来像“P2 移动距离比 P1 少”。
	this.x=(WIDTH/4-BOX_SIZE)/2;
	this.y=40+((conv-1)%8)*(BOX_SIZE+18);
	this.dir=DIRECTION.STOP;
	this.speed=80;
}
GameObject.prototype.move=function(dtMs){
	var dt=dtMs/1000;
	if(this.dir===DIRECTION.UP)this.y-=this.speed*dt;
	else if(this.dir===DIRECTION.DOWN)this.y+=this.speed*dt;
	else if(this.dir===DIRECTION.LEFT)this.x-=this.speed*dt;
	else if(this.dir===DIRECTION.RIGHT)this.x+=this.speed*dt;
	if(this.x<0)this.x=0; if(this.x>WIDTH-BOX_SIZE)this.x=WIDTH-BOX_SIZE;
	if(this.y<0)this.y=0; if(this.y>HEIGHT-BOX_SIZE)this.y=HEIGHT-BOX_SIZE;
};

// ============================================================
// Client —— 对应 kfs_client_core.c（每标签页 = 一个真实客户端）
// ============================================================
function Client(wsUrl, roomId, conv, token, heartbeatMs, stepMs){
	this.wsUrl=wsUrl; this.roomId=roomId; this.conv=conv; this.token=token;
	this.heartbeatMs=heartbeatMs; this.stepMs=stepMs;
	this.ws=null;
	this.joined=false;      // 已收到服务器 JOIN 应答
	this.joinOk=false;      // JOIN 是否成功
	this.result=-1;
	this.state={};          // 本地确定性状态 conv -> GameObject
	this.recvQueue=[];      // 收到的广播帧（未消化）
	this.currentDir=DIRECTION.STOP;
	this.nextFid=0;         // 裁决帧号（服务器已广播的最小公共帧）
	this.sendFid=-1;        // 已发出的最高目标帧
	this.advanceFrames=2;   // 输入提前量窗口（对齐 config advance_frames）
	this.lastBfid=-1;
	this.lastBroadcastAt=0;   // 最近一次收到服务器广播的时间戳（用于判断是否在推帧）
	this.readyAt=0;           // JOIN 成功/READY 的时间（用于“迟迟没有广播”提示）
	this.noFrameWarned=false;
	this.lastVerify="";
	this.logs=[];
	this._dropped=false;    // 模拟掉线开关
	this.readySent=false;
}
Client.prototype.log=function(s){ this.logs.unshift(s); if(this.logs.length>40)this.logs.pop(); renderStatus(); };

// ---------- WebSocket 生命周期 ----------
Client.prototype.connect=function(){
	var self=this;
	this.log("正在连接网关 " + this.wsUrl + " ...");
	setConnStatus("⏳ 正在连接网关 " + this.wsUrl + " ...", "info");
	try{ this.ws=new WebSocket(this.wsUrl); }
	catch(e){
		this.log("WebSocket 地址无效: "+e.message);
		setConnStatus("❌ WebSocket 地址无效: "+this.wsUrl, "err");
		return;
	}
	this.ws.onopen=function(){
		self.log("网关已连接，发送 JOIN room_id="+self.roomId+" conv="+self.conv+" token="+self.token);
		setConnStatus("✅ 网关已连接，正在 JOIN room="+self.roomId+" conv="+self.conv+" ...", "info");
		self.send({op:"join", room_id:self.roomId, conv:self.conv, token:self.token, heartbeat_ms:self.heartbeatMs});
	};
	this.ws.onmessage=function(ev){ self.onMsg(JSON.parse(ev.data)); };
	this.ws.onclose=function(){
		self.joined=false; self.joinOk=false;
		if(self._suppressStatus){ renderStatus(); return; }
		if(!self._dropped){
			self.log("网关连接已断开");
			setConnStatus("❌ 网关连接已断开（检查 gateway.js 是否还在运行）", "err");
		}else{
			self.log("已断开（模拟掉线，服务器 idle_timeout 后会判掉线，席位保留）");
			setConnStatus("⚠ 已断开（模拟掉线）", "warn");
		}
		renderStatus();
	};
	this.ws.onerror=function(){
		self.log("WebSocket 错误（网关未启动？检查 gateway.js 是否运行）");
		setConnStatus("❌ WebSocket 错误：网关未启动？先运行 node gateway.js", "err");
	};
};
Client.prototype.send=function(obj){
	if(this.ws && this.ws.readyState===1) this.ws.send(JSON.stringify(obj));
};
Client.prototype.close=function(){
	this._dropped=true;
	if(this.ws){ try{ this.ws.close(); }catch(e){} this.ws=null; }
	this.joined=false; this.joinOk=false;
	this.log("连接已关闭（模拟掉线）");
};

// ---------- 网关消息处理 ----------
Client.prototype.onMsg=function(msg){
	var self=this;
	switch(msg.op){
		case "joined":
			this.joined=true; this.joinOk=msg.ok; this.result=msg.result;
			if(msg.ok){
				this.log("JOIN 成功 result=0，发送 READY 就绪...");
				if(!this.state[this.conv]) this.state[this.conv]=new GameObject(this.conv);
				this.sendReady();
				this.readyAt=Date.now(); this.noFrameWarned=false;
				setConnStatus("✅ JOIN 成功，已上报 READY，等待服务器广播...", "ok");
				showTips("JOIN 成功，已上报 READY，等待服务器推帧...");
			}else{
				var names=["成功","房间不存在","房间已满","conv冲突","其他错误","token错误","超时"];
				var rname=(msg.result===-1)?"超时":(names[msg.result]||"未知");
				this.log("JOIN 被拒: result="+msg.result+" ("+rname+")"+(msg.msg?(" — "+msg.msg):""));
				setConnStatus("❌ JOIN 失败: result="+msg.result+" ("+rname+")"+(msg.result===5?" — token 过期":"")+"。点「返回登录」→「① 读取当前 token」后重试。", "err");
				showTips("JOIN 失败: result="+msg.result+" ("+rname+")"+(msg.result===5?" — token 过期，点「读取当前 token」":""));
				var st=document.getElementById("status");
				if(st && st.scrollIntoView) st.scrollIntoView({behavior:"smooth", block:"nearest"});
			}
			renderStatus();
			break;
		case "broadcast":
			this.onBroadcast(msg);
			break;
		case "log":
			this.log("[网关] "+msg.msg);
			break;
		case "error":
			this.log("[网关错误] "+msg.msg);
			break;
		case "pong":
			break;
	}
};

// ---------- 就绪 / 输入 / 状态上报 ----------
Client.prototype.sendReady=function(){
	if(!this.joinOk) return;
	this.send({op:"ready"});
	this.readySent=true;
	this.log("已上报 READY（等待 server.lua room:start() 开始推帧）");
};
// 输入提前量：把输入补发到 next_fid + advance_frames（对齐 client.lua topup）
Client.prototype.topup=function(){
	if(!this.joinOk) return;
	var target=this.nextFid+this.advanceFrames;
	// 关键：新客户端第一次收到很高的 frame_id（房间已经推了很久）时，
	// sendFid 还停在 0..2。不要从旧帧开始补发几千帧——服务器只接受
	// collecting_fid 附近 8 帧，补发旧帧会堵塞 KCP，当前方向输入迟迟到不了。
	if(this.sendFid < this.nextFid - 1){
		this.sendFid=this.nextFid-1;
	}
	while(this.sendFid<target){
		this.sendFid++;
		if(this.sendFid>=0){
			this.send({op:"input", fid:this.sendFid, data:this.dirToHex(this.currentDir)});
		}
	}
};
// 输入编码：发送 ASCII 数字 '0'..'4'（0x30..0x34），与 server.lua / client.lua
// 的文本输入格式一致。之前发的是二进制 0x00..0x04，广播回来
// String.fromCharCode(0x01) 不是可 parseInt 的数字，方向永远被当成 STOP。
Client.prototype.dirToHex=function(d){
	var code=0x30 + (Number(d)&0x0f);
	return ("0"+code.toString(16)).slice(-2);
};
Client.prototype.reportState=function(fid, payloadBytes){
	// 对齐 client.lua：h=(h*31+byte)%2^32，对完整广播负载（含尾 4B 对账哈希）
	var h=0;
	for(var i=0;i<payloadBytes.length;i++){ h=(h*31+payloadBytes[i])%4294967296; }
	this.send({op:"state", fid:fid, hash:h>>>0});
};

// ---------- 广播帧处理 ----------
Client.prototype.onBroadcast=function(msg){
	var fid=msg.fid, ok=msg.ok;
	var first=!this.lastBroadcastAt;
	this.lastBroadcastAt=Date.now();
	if(first){
		setConnStatus("🎮 已收到服务器广播 fid="+fid+"，帧同步运行中", "ok");
		showTips("已收到服务器广播 fid="+fid+"，帧同步运行中");
	}
	var payload=hexToBytes(msg.payload);
	// 解析逐玩家 [conv 4B][len 2B][data]；末尾 4B 为服务器对账哈希（跳过）
	var players=[], off=0, bodyLen=payload.length-4;
	if(bodyLen>=6){
		while(off+6<=bodyLen){
			var conv=(payload[off]|payload[off+1]<<8|payload[off+2]<<16|payload[off+3]<<24)>>>0; off+=4;
			var len=payload[off]|payload[off+1]<<8; off+=2;
			if(off+len>bodyLen) break;
			var data=payload.subarray(off, off+len); off+=len;
			players.push({conv:conv, data:data});
		}
	}
	var brief=[];
	for(var i=0;i<players.length;i++){ brief.push("c"+players[i].conv+":"+String.fromCharCode.apply(null,players[i].data)); }
	this.log("收到广播 fid="+fid+" 玩家数="+players.length+" 对账="+(ok?"一致":"不一致!!")+" ["+brief.join(" ")+"]");
	this.lastBfid=fid;
	this.lastVerify=ok?("帧"+fid+" 服务器对账哈希一致"):("!! 帧"+fid+" 服务器对账哈希不一致 DESYNC !!");
	if(!ok){ this.log("!! 帧 "+fid+" 对账不一致（输入哈希不同，服务器侧判不同步）"); }

	// 本地确定性状态推进：应用该帧全员输入（data 即方向数字字符串）
	for(var j=0;j<players.length;j++){
		var p=players[j];
		if(p.data.length>0){
			if(!this.state[p.conv]) this.state[p.conv]=new GameObject(p.conv);
			var d=parseInt(String.fromCharCode.apply(null,p.data),10);
			this.state[p.conv].dir=isNaN(d)?DIRECTION.STOP:d;
			this.state[p.conv].move(this.stepMs);
		}
	}
	// 上报状态哈希（对账）
	this.reportState(fid, payload);
	this.recvQueue.push(fid);
	// 裁决帧号推进：本客户端已收到该帧 → 可补发下一批输入（锁步由服务器裁决）
	if(fid+1>this.nextFid){ this.nextFid=fid+1; this.topup(); }
	renderStatus();
};

// 按帧消化（返回是否有可消化帧）
Client.prototype.stepForward=function(){
	return this.recvQueue.length>0;
};

// ============================================================
// 主控与 UI
// ============================================================
var client=null, viewConv=1, ctx=null;

function setConnStatus(text, kind){
	var el=$("#conn_status");
	if(!el.length) return;
	el.removeClass("ok err warn info").addClass(kind||"info").text(text);
}

// 从网关读取容器内当前 tokens_<room>.txt（网关侧 docker exec 读取）
// 解决“server.lua 每次重启 token 都变、手动抄旧 token 导致 JOIN result=5”的问题。
function fetchTokens(onDone, silent){
	var wsUrl=$("#ws_url").val().trim()||"ws://127.0.0.1:18080";
	var roomId=parseInt($("#room_id").val(),10); if(isNaN(roomId)) roomId=1;
	var wantConv=parseInt($("#conv").val(),10); if(isNaN(wantConv)) wantConv=101;
	var ws, done=false;
	var finish=function(hit){ if(typeof onDone==="function") onDone(hit); };
	var timer=setTimeout(function(){
		if(done) return;
		done=true;
		try{ ws.close(); }catch(e){}
		if(!silent) showTips("读取 token 超时：网关没启动？");
		finish(null);
	},4000);
	try{ ws=new WebSocket(wsUrl); }
	catch(e){
		clearTimeout(timer);
		if(!silent) showTips("网关地址无效: "+wsUrl);
		finish(null);
		return;
	}
	ws.onopen=function(){ ws.send(JSON.stringify({op:"tokens", room_id:roomId})); };
	ws.onmessage=function(ev){
		var m; try{ m=JSON.parse(ev.data); }catch(e){ return; }
		if(m.op!=="tokens" || done) return;
		done=true; clearTimeout(timer); try{ ws.close(); }catch(e){}
		if(m.error){
			if(!silent) showTips("读取 token 失败: "+m.error);
			finish(null); return;
		}
		if(!m.tokens || !m.tokens.length){
			if(!silent) showTips("房间 "+roomId+" 没有 token 文件，确认 server.lua 已启动");
			finish(null); return;
		}
		var hit=null;
		for(var i=0;i<m.tokens.length;i++){ if(m.tokens[i].conv===wantConv){ hit=m.tokens[i]; break; } }
		if(!hit) hit=m.tokens[0];
		$("#conv").val(hit.conv);
		$("#token").val(hit.token);
		var fr=Number(m.frame_rate||0);
		var stepMs=0;
		if(fr>0){
			stepMs=Math.round(1000/fr);
			$("#step_ms").val(stepMs);
		}
		if(!silent){
			showTips("已填入 conv="+hit.conv+" token="+hit.token+
				(stepMs?("；按 frame_rate="+fr+" 设帧间隔 "+stepMs+"ms"):"")+
				"，请再点「② 连接并对局」");
		}
		finish(hit);
	};
	ws.onerror=function(){
		if(done) return;
		done=true; clearTimeout(timer);
		if(!silent) showTips("读取 token 失败：网关未启动？");
		finish(null);
	};
}

function fetchTokensClick(){
	fetchTokens(null, false);
}

// 返回登录（由 index.html 的 inline onclick 调用，不依赖 jQuery ready 绑定）
function backToLogin(){
	if(client){
		client._suppressStatus=true;
		client._dropped=true;
		if(client.ws){ try{ client.ws.close(); }catch(e){} }
		client=null;
	}
	$("#content").hide(); $("#login").show();
	$("#reconnect_btn").hide();
	setConnStatus("未连接", "info");
	renderStatus();
}

function startClient(){
	var wsUrl=$("#ws_url").val().trim()||"ws://127.0.0.1:18080";
	// 防混淆：10500 是服务器 UDP 端口（网关去连的），浏览器只连网关 WebSocket 18080（或本机网关 8081）
	if(wsUrl.indexOf(":10500")>=0 || wsUrl.indexOf(":10500/")>=0){
		showTips("网关地址填错了：10500 是服务器 UDP 端口，浏览器要连网关 WebSocket 18080（本机网关则 8081）");
		return;
	}
	var roomId=parseInt($("#room_id").val(),10); if(isNaN(roomId)) roomId=1;
	var conv=parseInt($("#conv").val(),10); if(isNaN(conv)) conv=101;
	var tokenText=$("#token").val().trim();
	var token=parseInt(tokenText,10); if(isNaN(token)) token=0;
	var heartbeatMs=parseInt($("#heartbeat_ms").val(),10); if(isNaN(heartbeatMs)||heartbeatMs<0) heartbeatMs=500;
	var stepMs=parseInt($("#step_ms").val(),10); if(isNaN(stepMs)||stepMs<10) stepMs=1000;

	// token 为空：先自动读取当前 token，再自动继续连接，点②就能直接进
	if(tokenText===""){
		setConnStatus("⏳ token 为空，正在读取当前 token...", "info");
		showTips("token 为空，正在自动读取当前 token...");
		fetchTokens(function(hit){
			if(!hit){
				showTips("读取 token 失败：请确认网关和 server.lua 已启动");
				setConnStatus("❌ 读取 token 失败：请确认网关和 server.lua 已启动", "err");
				return;
			}
			startClient();
		}, true);
		return;
	}

	// 重连/重新开始时先静默关掉旧客户端，避免旧 onclose 覆盖新状态条
	if(client){
		client._suppressStatus=true;
		if(client.ws){ try{ client.ws.close(); }catch(e){} }
		client=null;
	}

	setConnStatus("⏳ 正在连接网关 "+wsUrl+" ...", "info");
	showTips("正在连接网关 "+wsUrl+" ...");
	$("#login").hide(); $("#content").show();

	client=new Client(wsUrl, roomId, conv, token, heartbeatMs, stepMs);
	client.log("启动客户端 conv="+conv+" room_id="+roomId+" token="+token);
	client.connect();
	renderStatus();
}

// 渲染：绘制"当前视角客户端"看到的确定性状态
function render(){
	ctx.clearRect(0,0,WIDTH,HEIGHT);
	var state=(client&&client.state)?client.state:{};
	var keys=Object.keys(state).sort();
	for(var i=0;i<keys.length;i++){
		var obj=state[keys[i]];
		ctx.fillStyle=COLORS[(obj.conv-1)%COLORS.length];
		ctx.fillRect(obj.x,obj.y,BOX_SIZE,BOX_SIZE);
		ctx.font="13px Courier New"; ctx.fillStyle="#fff";
		ctx.fillText("C"+obj.conv,obj.x+1,obj.y+BOX_SIZE-6);
	}
}
function renderStatus(){
	if(!client){ $("#status").text(""); return; }
	var s="conv="+client.conv+" | room="+client.roomId+" | token="+client.token;
	s+="\n连接: "+(client.ws?(client.ws.readyState===1?"已连接":"连接中/断开"):"未连接");
	s+=" | JOIN: "+(client.joinOk?"成功":(client.result===-1?"超时":"未成功(result="+client.result+")"));
	s+=" | 就绪: "+(client.readySent?"已上报":"未上报");
	s+=" | 裁决帧: "+client.nextFid+" | 已发至: "+client.sendFid+" | 最近广播: "+client.lastBfid+"\n";
	var pushHint = client.lastBroadcastAt ? (Math.round((Date.now()-client.lastBroadcastAt)/100)/10+"s前") : "尚未收到";
	s+="服务器推帧: "+pushHint+(client.lastBroadcastAt && Date.now()-client.lastBroadcastAt>2000?"  ⚠ 超过2s未推帧（服务器没跑/没start/人没齐）":"")+"\n";
	s+=client.lastVerify+"\n";
	s+="--- 本地确定性状态 ---\n";
	var keys=Object.keys(client.state).sort();
	for(var i=0;i<keys.length;i++){ var o=client.state[keys[i]]; s+="C"+o.conv+" ("+Math.round(o.x)+","+Math.round(o.y)+") dir="+o.dir+"\n"; }
	s+="--- 最近日志 ---\n"+client.logs.slice(0,8).join("\n");
	$("#status").text(s);
}

$(function(){
	var canvas=document.getElementById("canvas");
	ctx=canvas.getContext("2d");
	$("#content").hide();

	// 键盘：方向键设置当前视角客户端的操作并立即补发；数字键1..N切换视角
	$("body").keydown(function(e){
		if(!client) return;
		if(e.keyCode>=49 && e.keyCode<=57){
			var n=e.keyCode-48;
			if(n>=1 && n<=8){
				viewConv=n;
				if(!client.state[n]) client.state[n]=new GameObject(n);
				renderStatus();
			}
			return;
		}
		var d=DIR_KEYS[e.keyCode];
		if(d!==undefined){
			e.preventDefault();   // 防止方向键滚动页面
			client.currentDir=d;
			client.topup();   // 立即把新方向补发到当前窗口
			renderStatus();
		}
	});
	$("body").keyup(function(e){
		var d=DIR_KEYS[e.keyCode];
		if(d!==undefined && client){
			e.preventDefault();
			client.currentDir=DIRECTION.STOP;
			client.topup();
			renderStatus();
		}
	});
	// ①/②/返回登录 由 index.html 的 inline onclick 调用：
	// 即使 jQuery ready 绑定失败或 main.js 被旧缓存，点按钮也一定有反应。

	// 改 conv 后自动重新读取该 conv 的 token（多人房第二个标签页切到 102 时很方便）
	$("#conv").on("change", function(){ fetchTokens(null, false); });

	// 页面打开后静默读取当前 token：网关/服务器已启动时，直接点②即可
	setTimeout(function(){
		if($("#token").val().trim()==="") {
			fetchTokens(function(hit){
				if(hit) showTips("已自动填入 token，帧间隔已按服务器 frame_rate 对齐；直接点「② 连接并对局」");
			}, true);
		}
	}, 600);

	// 注入 DESYNC：偏移当前视角玩家本地位置 → 上报哈希与全员不同 → 服务器对账报 DESYNC
	$("#desync_btn").click(function(){
		if(!client) return;
		var obj=client.state[viewConv];
		if(!obj){ showTips("还没有该玩家状态"); return; }
		obj.x+=5;
		client.log("已向 C"+viewConv+" 本地状态注入偏移(x+5)→ 服务器对账应报 DESYNC");
		showTips("已注入状态偏移(x+5)，服务器对账应报 DESYNC");
	});
	// 模拟掉线/重连：断开网关连接（服务器 idle_timeout 后判掉线，席位保留）
	$("#drop_btn").click(function(){
		if(!client) return;
		if(client.ws && client.ws.readyState===1){
			client.close();
			$("#reconnect_btn").show();
			showTips('已断开连接（模拟掉线），席位保留，点「重连」恢复');
		}else{
			showTips("当前已不在线");
		}
	});
	// 重连（对应断线重连：重新 join 同 room/conv/token，席位保留）
	$("#reconnect_btn").click(function(){
		if(!client) return;
		$("#reconnect_btn").hide();
		client._dropped=false;
		client.connect();
		showTips("正在重连（重新 JOIN，席位保留）");
	});
	// 错误 token 入房：另开一个临时客户端用错误 token 请求 JOIN，验证被服务器拒绝
	$("#badtoken_btn").click(function(){
		var wsUrl=$("#ws_url").val().trim()||"ws://127.0.0.1:18080";
		var roomId=parseInt($("#room_id").val(),10); if(isNaN(roomId)) roomId=100;
		var bad=new Client(wsUrl, roomId, 99, 12345, 500, 1000);
		bad.log("用错误 token(12345) 请求 JOIN room="+roomId+" conv=99（验证服务器鉴权拒绝）");
		bad.connect();
		showTips("已用错误 token 请求 JOIN，服务器应回 result=5(token错误)");
	});

	// 帧循环：绘制 + 状态栏刷新
	setInterval(function(){
		if(client && client.joinOk){
			client.topup();   // 保持输入窗口（键盘不操作时补 STOP 空操作包）
			renderStatus();
			// JOIN/READY 后迟迟没有广播：给出明确提示，而不是让 canvas 一直空着
			if(!client.lastBroadcastAt && client.readyAt && !client.noFrameWarned &&
			   Date.now()-client.readyAt>2500){
				client.noFrameWarned=true;
				setConnStatus("⚠ JOIN 成功，但 2.5s 没收到广播：检查 server.lua 是否已 start（单人房 1/1 应自动 start）", "warn");
			}
		}
		render();
	},33);
});

function showTips(str){
	var maxW=Math.min(620, $(window).width()-40);
	var w=Math.min(str.length*14+40, maxW);
	var hw=($(window).width())/2, hh=($(window).height())/2;
	$("#tips").stop(true,true).show().text(str).css({
		width:w, left:hw-w/2, top:hh,
		"line-height":"22px", height:"auto", padding:"12px 16px"
	}).animate({top:hh-100},300).fadeOut(1200);
}
