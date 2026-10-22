// ============================================================
// 坦克对战 · 帧同步验证 Demo —— WebSocket 真客户端（经网关连 Docker kfs 服务器）
//
// 链路：浏览器(WebSocket) ⇄ gateway.js(本机 Node) ⇄ UDP/KCP ⇄ Docker kfs 服务器
// （中转桥已移除，网关直连服务器 UDP 端口）
//
// 协议对齐 kfs 库真实客户端（kfs_client_core.c / client.lua）：
//   1. 登录填 room_id / conv / token → 网关代发裸 UDP JOIN 握手
//   2. joined 成功后 send_ready()（等 server.lua room:start 推帧）
//   3. 输入提前量：topup() 补发到 next_fid + advance_frames(=2)
//   4. 收到广播 → 解析逐玩家输入 → 本地确定性推进坦克/子弹 → 上报 STATE 哈希
//   5. 心跳由网关按 heartbeat_ms 自动补发
//
// 玩法（确定性，全员同输入同种子 → 同结果）：
//   · 地图带固定障碍；坦克有朝向炮管，可发射子弹
//   · 每人 lives 条命（登录可配，默认 3），一次中弹=死一条命；初始位置按玩家序号 p1,p2.. 固定槽位
//   · 死亡后 2s（按帧数换算，非真实时钟）在空闲槽位确定性随机复活；命耗尽=淘汰不再复活
//   · 子弹击杀计入 killer 统计；仅剩 1 名仍有命的玩家（且房内 ≥2 人）→ 判胜并停止发包
//
// 输入编码（2 字节 ASCII，服务器透明搬运并进对账哈希）：
//   byte0 = 方向 '0'..'4'（STOP/UP/DOWN/LEFT/RIGHT）  byte1 = 开火 '0'/'1'
// ============================================================

var DIRECTION = { STOP:0, UP:1, DOWN:2, LEFT:3, RIGHT:4 };
var DIR_KEYS   = { 38:DIRECTION.UP, 40:DIRECTION.DOWN, 37:DIRECTION.LEFT, 39:DIRECTION.RIGHT, 13:DIRECTION.STOP };
var FIRE_KEYS  = { 32:true };            // 空格开火

var WIDTH=600, HEIGHT=420, BOX_SIZE=26;
var COLORS=["#e74c3c","#3498db","#27ae60","#9b59b6","#f39c12","#1abc9c","#e67e22","#2c3e50",
            "#c0392b","#2980b9","#16a085","#8e44ad","#d35400","#f1c40f","#7f8c8d","#16a085"];

// ---------- 地图障碍（固定矩形，跨端一致；已避开 32 个出生槽）----------
var BARRIERS=[
	{x:150,y:70, w:40,h:40},{x:410,y:70,w:40,h:40},
	{x:150,y:310,w:40,h:40},{x:410,y:310,w:40,h:40},
	{x:260,y:180,w:80,h:60},
	{x:90,y:190,w:40,h:40},{x:470,y:190,w:40,h:40},
	{x:280,y:70,w:40,h:40},{x:280,y:310,w:40,h:40}
];

// ---------- 32 个固定出生槽（上8/下8/左8/右8，避开障碍与边界）----------
var SLOT_POS=(function(){
	var s=[], i, m=6;
	for(i=0;i<8;i++) s.push({x:Math.round(m+i*(WIDTH-2*m-BOX_SIZE)/7), y:m});
	for(i=0;i<8;i++) s.push({x:Math.round(m+i*(WIDTH-2*m-BOX_SIZE)/7), y:HEIGHT-m-BOX_SIZE});
	for(i=0;i<8;i++) s.push({x:m, y:Math.round(m+8+i*(HEIGHT-2*m-16-BOX_SIZE)/7)});
	for(i=0;i<8;i++) s.push({x:WIDTH-m-BOX_SIZE, y:Math.round(m+8+i*(HEIGHT-2*m-16-BOX_SIZE)/7)});
	return s;
})();

var VDX=[0,0,0,-1,1], VDY=[0,-1,1,0,0];

// ---------- 工具 ----------
function hexToBytes(hex){
	var out=new Uint8Array(hex.length/2);
	for(var i=0;i<out.length;i++){ out[i]=parseInt(hex.substr(i*2,2),16); }
	return out;
}
function clamp(v,lo,hi){ return v<lo?lo:(v>hi?hi:v); }
function rectsOverlap(ax,ay,aw,ah,bx,by,bw,bh){
	return ax<bx+bw && ax+aw>bx && ay<by+bh && ay+ah>by;
}
// 确定性伪随机（mulberry32）：由 (seed,conv,counter) 派生，全员一致
function seededRand(a){
	a=(a>>>0);
	a=Math.imul(a^(a>>>15), a|1);
	a^=a+Math.imul(a^(a>>>7), a|61);
	return ((a^(a>>>14))>>>0)/4294967296;
}

// ---------- 坦克（确定性游戏对象）----------
function Tank(conv, slotIdx, lives){
	this.conv=conv;
	this.slot=slotIdx;
	this.x=SLOT_POS[slotIdx%SLOT_POS.length].x;
	this.y=SLOT_POS[slotIdx%SLOT_POS.length].y;
	this.dir=DIRECTION.STOP;
	this.face=DIRECTION.UP;            // 炮管朝向（最后有效方向）
	this.speed=90;
	this.lives=lives;         // 剩余命：一次中弹=死一条命；0=淘汰不再复活
	this.livesMax=lives;
	this.alive=true;
	this.kills=0;
	this.deathFrames=0;                // 死亡后复活倒计时（帧）
	this.respawnCount=0;               // 已复活次数（喂 RNG）
	this.lastFire=0;                   // 上帧开火位（边沿检测）
	this.cdFrames=0;                   // 射击冷却（帧）
}

// ---------- 世界常量（由 stepMs 换算成"帧数"，保证确定性）----------
var RESPAWN_MS=2000, SHOT_CD_MS=400, BULLET_SPEED=300, BULLET_SIZE=6;

// ============================================================
// Client —— 对应 kfs_client_core.c（每标签页 = 一个真实客户端）
// ============================================================
function Client(wsUrl, roomId, conv, token, heartbeatMs, stepMs, lives){
	this.wsUrl=wsUrl; this.roomId=roomId; this.conv=conv; this.token=token;
	this.heartbeatMs=heartbeatMs; this.stepMs=stepMs; this.lives=lives||3;
	this.respawnFrames=Math.max(1, Math.round(RESPAWN_MS/this.stepMs));
	this.cdFrames=Math.max(1, Math.round(SHOT_CD_MS/this.stepMs));
	this.ws=null;
	this.joined=false; this.joinOk=false; this.result=-1;
	this.state={};            // conv -> Tank
	this.bullets=[];          // {owner,x,y,vx,vy}
	this.players=[];          // 本帧全员输入 [{conv,dir,fire}]
	this.sortedConvs=[];      // 稳定玩家序（决定 slot / p1..p32）
	this.roomSeed=0;          // 服务器锚定的房间种子（重连时补）
	this.recvQueue=[];
	this.currentDir=DIRECTION.STOP;
	this.currentFire=0;
	this.nextFid=0; this.sendFid=-1; this.advanceFrames=2;
	this.lastBfid=-1; this.lastBroadcastAt=0;
	this.readyAt=0; this.noFrameWarned=false; this.lastVerify="";
	this.logs=[]; this._dropped=false; this.readySent=false;
	this.gameOver=false; this.winnerConv=0;
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
		self.log("WebSocket 错误：连不上网关 "+self.wsUrl);
		setConnStatus("❌ WebSocket 错误：连不上网关 "+self.wsUrl+"（18080 需容器内网关；8081 需本机 node gateway.js）", "err");
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
	switch(msg.op){
		case "joined":
			this.joined=true; this.joinOk=msg.ok; this.result=msg.result;
			if(typeof msg.room_seed==="number") this.roomSeed=msg.room_seed>>>0;
			if(msg.ok){
				this.log("JOIN 成功 result=0，发送 READY 就绪...");
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
		case "rejoin_sync":
			this.roomSeed=msg.room_seed>>>0;
			this.log("收到重连锚点 seed="+this.roomSeed+" collect_fid="+msg.collect_fid);
			break;
		case "log":  this.log("[网关] "+msg.msg); break;
		case "error":this.log("[网关错误] "+msg.msg); break;
		case "pong": break;
	}
};

// ---------- 就绪 / 输入 / 状态上报 ----------
Client.prototype.sendReady=function(){
	if(!this.joinOk) return;
	this.send({op:"ready"});
	this.readySent=true;
	this.log("已上报 READY（等待 server.lua 开始推帧）");
};
Client.prototype.topup=function(){
	if(!this.joinOk) return;
	if(this.gameOver) return;           // 剩一人：停止发包
	var target=this.nextFid+this.advanceFrames;
	if(this.sendFid < this.nextFid - 1){ this.sendFid=this.nextFid-1; }
	while(this.sendFid<target){
		this.sendFid++;
		if(this.sendFid>=0){
			this.send({op:"input", fid:this.sendFid, data:this.inputToHex(this.myInput())});
		}
	}
};
// 本端只给"自己 conv"注入本机输入；视角切到别人时本端不再代发输入
Client.prototype.myInput=function(){
	return {dir:this.currentDir, fire:this.currentFire};
};
// 2 字节 ASCII 编码：[dir '0'..'4'][fire '0'/'1']
Client.prototype.inputToHex=function(inp){
	var d=0x30 + (Number(inp.dir)&0x0f);
	var f=0x30 + (inp.fire?1:0);
	return ("0"+d.toString(16)).slice(-2) + ("0"+f.toString(16)).slice(-2);
};
Client.prototype.reportState=function(fid, payloadBytes){
	var h=0;
	for(var i=0;i<payloadBytes.length;i++){ h=(h*31+payloadBytes[i])%4294967296; }
	this.send({op:"state", fid:fid, hash:h>>>0});
};

// ---------- 广播帧处理 + 确定性世界推进 ----------
Client.prototype.onBroadcast=function(msg){
	var fid=msg.fid, ok=msg.ok;
	var first=!this.lastBroadcastAt;
	this.lastBroadcastAt=Date.now();
	if(first){
		setConnStatus("🎮 已收到服务器广播 fid="+fid+"，帧同步运行中", "ok");
		showTips("已收到服务器广播 fid="+fid+"，坦克对战帧同步运行中");
	}
	var payload=hexToBytes(msg.payload);
	// 逐玩家 [conv 4B][len 2B][data]；末尾 4B 对账哈希（跳过）
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
	this.lastBfid=fid;
	this.lastVerify=ok?("帧"+fid+" 服务器对账哈希一致"):("!! 帧"+fid+" 服务器对账哈希不一致 DESYNC !!");
	if(!ok){ this.log("!! 帧 "+fid+" 对账不一致（输入哈希不同，服务器侧判不同步）"); }

	// 解码本帧全员输入（2 字节）
	var frameInputs=[];
	for(var j=0;j<players.length;j++){
		var p=players[j], dir=DIRECTION.STOP, fire=0;
		if(p.data.length>=1) dir=clamp(p.data[0]-0x30,0,4);
		if(p.data.length>=2) fire=(p.data[1]-0x30)?1:0;
		frameInputs.push({conv:p.conv, dir:dir, fire:fire});
	}
	this.ensurePlayers(frameInputs);
	this.stepWorld(frameInputs);
	if(!this.gameOver) this.reportState(fid, payload);

	this.recvQueue.push(fid);
	if(fid+1>this.nextFid){ this.nextFid=fid+1; this.topup(); }
	renderStatus();
};

// 玩家集合（按 conv 升序）→ 稳定 slot（p1=slot0..p32）
Client.prototype.ensurePlayers=function(frameInputs){
	var seen={}, i;
	for(i=0;i<frameInputs.length;i++) seen[frameInputs[i].conv]=1;
	if(!seen[this.conv]) seen[this.conv]=1;         // 保证自己始终在集合里
	var list=[]; for(var k in seen) list.push(Number(k));
	list.sort(function(a,b){return a-b;});
	var changed=list.length!==this.sortedConvs.length;
	for(i=0;i<list.length && !changed;i++){ if(list[i]!==this.sortedConvs[i]) changed=true; }
	this.sortedConvs=list;
	if(changed){
		for(i=0;i<list.length;i++){
			var conv=list[i];
			if(!this.state[conv]) this.state[conv]=new Tank(conv, this.freeSlot(), this.lives);
		}
		this.log("玩家就位："+list.map(function(c,ix){return "P"+(ix+1)+"=c"+c;}).join(" "));
	}
};
Client.prototype.slotOf=function(conv){
	var i=this.sortedConvs.indexOf(conv);
	return i<0?(conv%SLOT_POS.length):i;
};
// 最低空闲槽（确定性：各端按相同状态与相同创建顺序算出相同结果）
Client.prototype.freeSlot=function(){
	var used={}, k, s;
	for(k in this.state){ if(this.state[k]) used[this.state[k].slot]=1; }
	for(s=0;s<SLOT_POS.length;s++){ if(!used[s]) return s; }
	return 0;
};

// 单帧确定性推进
Client.prototype.stepWorld=function(frameInputs){
	var dt=this.stepMs/1000, i, inp, t;

	// 1) 应用全员输入
	for(i=0;i<frameInputs.length;i++){
		inp=frameInputs[i]; t=this.state[inp.conv];
		if(!t) continue;
		t.dir=inp.dir;
		if(inp.dir!==DIRECTION.STOP) t.face=inp.dir;
		if(!t.alive) { t.lastFire=inp.fire; continue; }
		if(inp.fire===1 && t.lastFire===0 && t.cdFrames<=0){
			this.spawnBullet(t);
			t.cdFrames=this.cdFrames;
		}
		t.lastFire=inp.fire;
	}
	// 2) 移动坦克（避障）
	for(i=0;i<this.sortedConvs.length;i++){
		t=this.state[this.sortedConvs[i]];
		if(!t||!t.alive) continue;
		this.moveTank(t, dt);
		if(t.cdFrames>0) t.cdFrames--;
	}
	// 3) 子弹推进 + 碰撞
	this.stepBullets(dt);
	// 4) 复活倒计时（命耗尽的淘汰者不参与）
	for(i=0;i<this.sortedConvs.length;i++){
		t=this.state[this.sortedConvs[i]];
		if(!t||t.alive||t.lives<=0) continue;
		if(--t.deathFrames<=0) this.respawn(t);
	}
	// 5) 胜负判定：房内≥2人且仅剩1人仍有命（其余全部淘汰）→ 停发包
	//    注意：死亡等待复活的玩家仍有命，不算淘汰，避免 2s 复活窗口内误判
	var inGameN=0;
	for(i=0;i<this.sortedConvs.length;i++){ if(this.state[this.sortedConvs[i]].lives>0) inGameN++; }
	if(this.sortedConvs.length>=2 && inGameN<=1 && !this.gameOver){
		this.gameOver=true;
		this.winnerConv=0;
		for(i=0;i<this.sortedConvs.length;i++){ if(this.state[this.sortedConvs[i]].lives>0){ this.winnerConv=this.sortedConvs[i]; } }
		this.log("★ 仅剩 1 名存活玩家（P"+(this.sortedConvs.indexOf(this.winnerConv)+1)+"=c"+this.winnerConv+"），胜负已分，停止发包");
		showTips("胜者：P"+(this.sortedConvs.indexOf(this.winnerConv)+1)+"（conv="+this.winnerConv+"）");
	}
};

Client.prototype.moveTank=function(t, dt){
	if(t.dir===DIRECTION.STOP) return;
	var nx=clamp(t.x+VDX[t.dir]*t.speed*dt,0,WIDTH-BOX_SIZE);
	var ny=clamp(t.y+VDY[t.dir]*t.speed*dt,0,HEIGHT-BOX_SIZE);
	if(!this.hitBarrier(nx,t.y,BOX_SIZE,BOX_SIZE)) t.x=nx;
	if(!this.hitBarrier(t.x,ny,BOX_SIZE,BOX_SIZE)) t.y=ny;
};
Client.prototype.hitBarrier=function(x,y,w,h){
	for(var i=0;i<BARRIERS.length;i++){
		var b=BARRIERS[i];
		if(rectsOverlap(x,y,w,h,b.x,b.y,b.w,b.h)) return true;
	}
	return false;
};
Client.prototype.spawnBullet=function(t){
	var cx=t.x+BOX_SIZE/2, cy=t.y+BOX_SIZE/2;
	var vx=VDX[t.face], vy=VDY[t.face];
	if(vx===0&&vy===0){ vx=0; vy=-1; }        // STOP 时用当前朝向（face 已保证非 STOP 语义）
	var off=BOX_SIZE/2+2;
	this.bullets.push({owner:t.conv, x:cx+vx*off, y:cy+vy*off, vx:vx, vy:vy});
};
Client.prototype.stepBullets=function(dt){
	var move=BULLET_SPEED*dt, out=[], i, j, b, t;
	for(i=0;i<this.bullets.length;i++){
		b=this.bullets[i];
		b.x+=b.vx*move; b.y+=b.vy*move;
		if(b.x<0||b.x>WIDTH||b.y<0||b.y>HEIGHT) continue;               // 越界消失
		if(this.hitBarrier(b.x,b.y,BULLET_SIZE,BULLET_SIZE)) continue;   // 撞墙消失
		var hit=false;
		for(j=0;j<this.sortedConvs.length;j++){
			t=this.state[this.sortedConvs[j]];
			if(!t||!t.alive||t.conv===b.owner) continue;                 // 自伤关闭：跳过 owner
			if(rectsOverlap(b.x,b.y,BULLET_SIZE,BULLET_SIZE,t.x,t.y,BOX_SIZE,BOX_SIZE)){
				this.damage(t, b.owner); hit=true; break;
			}
		}
		if(!hit) out.push(b);
	}
	this.bullets=out;
};
Client.prototype.damage=function(target, killerConv){
	var killer=this.state[killerConv];
	if(killer) killer.kills++;
	target.alive=false;
	target.lives--;
	if(target.lives<=0){
		target.lives=0;
		target.deathFrames=0;                           // 命耗尽：淘汰，不再复活
		this.log("☠ P"+(this.sortedConvs.indexOf(target.conv)+1)+"(c"+target.conv+") 被 P"+(this.sortedConvs.indexOf(killerConv)+1)+"(c"+killerConv+") 击杀，命耗尽淘汰（击杀者累计"+killer.kills+"杀）");
	}else{
		target.deathFrames=this.respawnFrames;          // 2s 后复活（按帧）
		this.log("☠ P"+(this.sortedConvs.indexOf(target.conv)+1)+"(c"+target.conv+") 被 P"+(this.sortedConvs.indexOf(killerConv)+1)+"(c"+killerConv+") 击杀，剩 "+target.lives+" 命（击杀者累计"+killer.kills+"杀）");
	}
};
Client.prototype.respawn=function(t){
	if(t.lives<=0) return;                              // 淘汰：不复活
	// 确定性随机空闲槽：由 (room_seed, conv, respawnCount) 派生，避开存活玩家占用的槽
	var base=Math.floor(seededRand((this.roomSeed ^ Math.imul(t.conv,2654435761) ^ Math.imul(t.respawnCount+1,40503))>>>0)*SLOT_POS.length);
	var idx=-1, k, i, occ;
	for(k=0;k<SLOT_POS.length;k++){
		i=(base+k)%SLOT_POS.length;
		occ=false;
		for(var c=0;c<this.sortedConvs.length;c++){
			var o=this.state[this.sortedConvs[c]];
			if(o&&o.alive&&o.slot===i){ occ=true; break; }
		}
		if(!occ){ idx=i; break; }
	}
	if(idx<0) idx=base;
	t.respawnCount++;
	t.slot=idx;
	t.x=SLOT_POS[idx].x; t.y=SLOT_POS[idx].y;
	t.alive=true; t.dir=DIRECTION.STOP; t.cdFrames=0;
	this.log("↑ P"+(this.sortedConvs.indexOf(t.conv)+1)+"(c"+t.conv+") 复活于槽"+idx);
};

// ============================================================
// 主控与 UI
// ============================================================
var client=null, viewIdx=0, ctx=null;

function setConnStatus(text, kind){
	var el=$("#conn_status");
	if(!el.length) return;
	el.removeClass("ok err warn info").addClass(kind||"info").text(text);
}

function fetchTokens(onDone, silent){
	var wsUrl=$("#ws_url").val().trim()||"ws://127.0.0.1:8081";
	var roomId=parseInt($("#room_id").val(),10); if(isNaN(roomId)) roomId=1;
	var wantConv=parseInt($("#conv").val(),10); if(isNaN(wantConv)) wantConv=101;
	var ws, done=false;
	var finish=function(hit){ if(typeof onDone==="function") onDone(hit); };
	var timer=setTimeout(function(){
		if(done) return;
		done=true;
		try{ ws.close(); }catch(e){}
		if(!silent) showTips("读取 token 超时：网关 "+wsUrl+" 无响应（容器网关 18080 需先跑 start_container.sh；本机网关 8081 需 node gateway.js）");
		finish(null);
	},6000);
	try{ ws=new WebSocket(wsUrl); }
	catch(e){ clearTimeout(timer); if(!silent) showTips("网关地址无效: "+wsUrl); finish(null); return; }
	ws.onopen=function(){ ws.send(JSON.stringify({op:"tokens", room_id:roomId})); };
	ws.onmessage=function(ev){
		var m; try{ m=JSON.parse(ev.data); }catch(e){ return; }
		if(m.op!=="tokens" || done) return;
		done=true; clearTimeout(timer); try{ ws.close(); }catch(e){}
		if(m.error){ if(!silent) showTips("读取 token 失败: "+m.error); finish(null); return; }
		if(!m.tokens || !m.tokens.length){ if(!silent) showTips("房间 "+roomId+" 没有 token 文件，确认 server.lua 已启动"); finish(null); return; }
		var hit=null;
		for(var i=0;i<m.tokens.length;i++){ if(m.tokens[i].conv===wantConv){ hit=m.tokens[i]; break; } }
		if(!hit) hit=m.tokens[0];
		$("#conv").val(hit.conv);
		$("#token").val(hit.token);
		var fr=Number(m.frame_rate||0);
		var stepMs=0;
		if(fr>0){ stepMs=Math.round(1000/fr); $("#step_ms").val(stepMs); }
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
		if(!silent) showTips("读取 token 失败：连不上网关 "+wsUrl+"（18080=容器内网关；8081=本机 node gateway.js）");
		finish(null);
	};
}
function fetchTokensClick(){ fetchTokens(null, false); }

function backToLogin(){
	if(client){
		client._suppressStatus=true; client._dropped=true;
		if(client.ws){ try{ client.ws.close(); }catch(e){} }
		client=null;
	}
	viewIdx=0;
	$("#content").hide(); $("#login").show();
	$("#reconnect_btn").hide();
	setConnStatus("未连接", "info");
	renderStatus();
}

function startClient(){
	var wsUrl=$("#ws_url").val().trim()||"ws://127.0.0.1:cc";
	if(wsUrl.indexOf(":10500")>=0){
		showTips("网关地址填错了：10500 是服务器 UDP 端口，浏览器要连网关 WebSocket 18080（本机网关则 8081）");
		return;
	}
	var roomId=parseInt($("#room_id").val(),10); if(isNaN(roomId)) roomId=1;
	var conv=parseInt($("#conv").val(),10); if(isNaN(conv)) conv=101;
	var tokenText=$("#token").val().trim();
	var token=parseInt(tokenText,10); if(isNaN(token)) token=0;
	var heartbeatMs=parseInt($("#heartbeat_ms").val(),10); if(isNaN(heartbeatMs)||heartbeatMs<0) heartbeatMs=500;
	var stepMs=parseInt($("#step_ms").val(),10); if(isNaN(stepMs)||stepMs<10) stepMs=100;
	var lives=parseInt($("#lives").val(),10); if(isNaN(lives)||lives<1) lives=3; if(lives>9) lives=9;

	if(tokenText===""){
		setConnStatus("⏳ token 为空，正在读取当前 token...", "info");
		showTips("token 为空，正在自动读取当前 token...");
		fetchTokens(function(){ startClient(); }, true);
		return;
	}

	if(client){
		client._suppressStatus=true;
		if(client.ws){ try{ client.ws.close(); }catch(e){} }
		client=null;
	}
	viewIdx=0;
	setConnStatus("⏳ 正在连接网关 "+wsUrl+" ...", "info");
	showTips("正在连接网关 "+wsUrl+" ...");
	$("#login").hide(); $("#content").show();

	client=new Client(wsUrl, roomId, conv, token, heartbeatMs, stepMs, lives);
	client.log("启动客户端 conv="+conv+" room_id="+roomId+" token="+token+" lives="+lives);
	client.connect();
	renderStatus();
}

// ---------- 渲染 ----------
function render(){
	if(!ctx) return;
	ctx.clearRect(0,0,WIDTH,HEIGHT);
	ctx.fillStyle="#20242a"; ctx.fillRect(0,0,WIDTH,HEIGHT);
	for(var i=0;i<BARRIERS.length;i++){
		var b=BARRIERS[i];
		ctx.fillStyle="#556"; ctx.fillRect(b.x,b.y,b.w,b.h);
		ctx.strokeStyle="#778"; ctx.strokeRect(b.x+0.5,b.y+0.5,b.w-1,b.h-1);
	}
	var state=(client&&client.state)?client.state:{};
	var convs=(client&&client.sortedConvs)?client.sortedConvs:Object.keys(state).map(Number);
	if(client){
		ctx.fillStyle="#f6e58d";
		for(var k=0;k<client.bullets.length;k++){
			var bl=client.bullets[k]; ctx.fillRect(bl.x-BULLET_SIZE/2,bl.y-BULLET_SIZE/2,BULLET_SIZE,BULLET_SIZE);
		}
	}
	for(var j=0;j<convs.length;j++){
		var t=state[convs[j]]; if(!t) continue;
		if(!t.alive){
			ctx.strokeStyle="rgba(255,255,255,.25)";
			ctx.strokeRect(t.x+0.5,t.y+0.5,BOX_SIZE-1,BOX_SIZE-1);   // 死亡占位
			continue;
		}
		var col=COLORS[j%COLORS.length];
		var isView=(client&&j===viewIdx);
		ctx.fillStyle="#333";
		ctx.fillRect(t.x,t.y+2,BOX_SIZE,4); ctx.fillRect(t.x,t.y+BOX_SIZE-6,BOX_SIZE,4);
		ctx.fillStyle=col; ctx.fillRect(t.x+3,t.y+6,BOX_SIZE-6,BOX_SIZE-12);
		// 炮管
		var cx=t.x+BOX_SIZE/2, cy=t.y+BOX_SIZE/2, L=BOX_SIZE*0.7;
		ctx.strokeStyle=col; ctx.lineWidth=3;
		ctx.beginPath(); ctx.moveTo(cx,cy); ctx.lineTo(cx+VDX[t.face]*L, cy+VDY[t.face]*L); ctx.stroke();
		ctx.lineWidth=1;
		if(isView){ ctx.strokeStyle="#fff"; ctx.strokeRect(t.x-1,t.y-1,BOX_SIZE+2,BOX_SIZE+2); }
		ctx.font="11px Courier New"; ctx.fillStyle="#fff";
		ctx.fillText("P"+(j+1), t.x, t.y-3);
		for(var h=0;h<t.livesMax;h++){
			ctx.fillStyle=(h<t.lives)?"#2ecc71":"#444";
			ctx.fillRect(t.x+h*6, t.y+BOX_SIZE+2, 5, 3);
		}
		if(t.kills>0){ ctx.fillStyle="#f6e58d"; ctx.fillText(t.kills+"K", t.x+BOX_SIZE-10, t.y+BOX_SIZE+10); }
	}
	if(client&&client.gameOver){
		ctx.fillStyle="rgba(0,0,0,.55)"; ctx.fillRect(0,HEIGHT/2-30,WIDTH,60);
		ctx.fillStyle="#fff"; ctx.font="20px Courier New"; ctx.textAlign="center";
		ctx.fillText("胜者 P"+(convs.indexOf(client.winnerConv)+1)+"  ·  已停止发包", WIDTH/2, HEIGHT/2+6);
		ctx.textAlign="left";
	}
}
function renderStatus(){
	if(!client){ $("#status").text(""); return; }
	var s="conv="+client.conv+" | room="+client.roomId+" | token="+client.token+" | lives="+client.lives;
	s+="\n连接: "+(client.ws?(client.ws.readyState===1?"已连接":"连接中/断开"):"未连接");
	s+=" | JOIN: "+(client.joinOk?"成功":(client.result===-1?"超时":"未成功(result="+client.result+")"));
	s+=" | 就绪: "+(client.readySent?"已上报":"未上报");
	s+=" | 裁决帧: "+client.nextFid+" | 已发至: "+client.sendFid+" | 最近广播: "+client.lastBfid;
	s+=" | "+(client.gameOver?("★ 胜负已分(停止发包)"):("在场"+aliveCount()+"/"+client.sortedConvs.length))+"\n";
	var pushHint = client.lastBroadcastAt ? (Math.round((Date.now()-client.lastBroadcastAt)/100)/10+"s前") : "尚未收到";
	s+="服务器推帧: "+pushHint+(client.lastBroadcastAt && Date.now()-client.lastBroadcastAt>2000 && !client.gameOver ?"  ⚠ 超过2s未推帧（服务器没跑/没start/人没齐）":"")+"\n";
	s+=client.lastVerify+"\n";
	s+="--- 玩家状态 ---\n";
	for(var i=0;i<client.sortedConvs.length;i++){
		var o=client.state[client.sortedConvs[i]]; if(!o) continue;
		s+=("P"+(i+1)+" c"+o.conv+(i===viewIdx?" *":"  ")+" 命"+o.lives+"/"+o.livesMax+" 杀"+o.kills+
			(o.lives<=0?" 淘汰":(o.alive?" 活":" 死(复活中)"))+" ("+Math.round(o.x)+","+Math.round(o.y)+") dir="+o.dir+"\n");
	}
	s+="--- 最近日志 ---\n"+client.logs.slice(0,8).join("\n");
	$("#status").text(s);
}
function aliveCount(){
	if(!client) return 0;
	var n=0; for(var i=0;i<client.sortedConvs.length;i++){ if(client.state[client.sortedConvs[i]].lives>0) n++; } return n;
}

$(function(){
	var canvas=document.getElementById("canvas");
	ctx=canvas.getContext("2d");
	$("#content").hide();

	$("body").keydown(function(e){
		if(!client) return;
		if(e.keyCode>=49 && e.keyCode<=56){
			viewIdx=e.keyCode-49;
			if(viewIdx>=client.sortedConvs.length) viewIdx=Math.max(0,client.sortedConvs.length-1);
			renderStatus();
			return;
		}
		if(FIRE_KEYS[e.keyCode]){
			e.preventDefault();
			client.currentFire=1; client.topup(); renderStatus();
			return;
		}
		var d=DIR_KEYS[e.keyCode];
		if(d!==undefined){
			e.preventDefault();
			client.currentDir=d;
			client.topup();
			renderStatus();
		}
	});
	$("body").keyup(function(e){
		if(!client) return;
		if(FIRE_KEYS[e.keyCode]){ e.preventDefault(); client.currentFire=0; client.topup(); renderStatus(); return; }
		var d=DIR_KEYS[e.keyCode];
		if(d!==undefined){
			e.preventDefault();
			client.currentDir=DIRECTION.STOP;
			client.topup();
			renderStatus();
		}
	});

	$("#conv").on("change", function(){ fetchTokens(null, false); });

	setTimeout(function(){
		if($("#token").val().trim()==="") {
			fetchTokens(function(hit){
				if(hit) showTips("已自动填入 token，帧间隔已按服务器 frame_rate 对齐；直接点「② 连接并对局」");
			}, true);
		}
	}, 600);

	$("#desync_btn").click(function(){
		if(!client) return;
		var conv=client.sortedConvs[viewIdx], obj=client.state[conv];
		if(!obj){ showTips("还没有该玩家状态"); return; }
		obj.x+=5;
		client.log("已向 P"+(viewIdx+1)+"(c"+conv+") 注入偏移(x+5)→ 服务器对账应报 DESYNC");
		showTips("已注入状态偏移(x+5)，服务器对账应报 DESYNC");
	});
	$("#drop_btn").click(function(){
		if(!client) return;
		if(client.ws && client.ws.readyState===1){
			client.close();
			$("#reconnect_btn").show();
			showTips('已断开连接（模拟掉线），席位保留，点「重连」恢复');
		}else{ showTips("当前已不在线"); }
	});
	$("#reconnect_btn").click(function(){
		if(!client) return;
		$("#reconnect_btn").hide();
		client._dropped=false;
		client.connect();
		showTips("正在重连（重新 JOIN，席位保留）");
	});
	$("#badtoken_btn").click(function(){
		var wsUrl=$("#ws_url").val().trim()||"ws://127.0.0.1:8081";
		var roomId=parseInt($("#room_id").val(),10); if(isNaN(roomId)) roomId=100;
		var bad=new Client(wsUrl, roomId, 99, 12345, 500, 100, 3);
		bad.log("用错误 token(12345) 请求 JOIN room="+roomId+" conv=99（验证服务器鉴权拒绝）");
		bad.connect();
		showTips("已用错误 token 请求 JOIN，服务器应回 result=5(token错误)");
	});

	setInterval(function(){
		if(client && client.joinOk){
			client.topup();
			renderStatus();
			if(!client.lastBroadcastAt && client.readyAt && !client.noFrameWarned &&
			   Date.now()-client.readyAt>2500){
				client.noFrameWarned=true;
				setConnStatus("⚠ JOIN 成功，但 2.5s 没收到广播：检查 server.lua 是否已 start（单人房应自动 start）", "warn");
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
