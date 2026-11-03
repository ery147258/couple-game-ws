/**
 * 情侣联机小游戏 · WebSocket 服务端
 * 部署目标：抖音云容器服务（容器内端口必须是 8000）
 *
 * 协议（JSON 文本帧）：
 *   客户端 → 服务端
 *     { type: "create_room" }                        创建房间
 *     { type: "join_room", roomId: "123456" }        加入房间（最多 2 人）
 *     { type: "relay", roomId: "123456", payload: ? } 把 payload 原样转发给同房间的另一个人
 *     { type: "ping" }                               应用层心跳（可选）
 *
 *   服务端 → 客户端
 *     { type: "room_created", roomId, clientId, count }
 *     { type: "joined",       roomId, clientId, count, peerReady }
 *     { type: "peer_joined",  roomId, count }        通知房主：对手进来了
 *     { type: "peer_left" }                          通知：对手断线/离开了
 *     { type: "relay",        payload }              转发来的 payload（原样，不加壳）
 *     { type: "pong",         t }
 *     { type: "error",        code, message, ... }   code 见下方 ERR
 *
 * 设计要点：
 *   - 同一个 8000 端口同时提供 HTTP 健康检查和 WebSocket 升级，
 *     云平台探活打 GET / 就能拿到 200，不会因为「不是 WS 请求」而失败。
 *   - 空房间立刻从 Map 里删掉，不做内存泄漏式保留。
 *   - 30 秒 ping/pong 探活，云上常见的「假连接」会被 terminate 掉并触发清理。
 */

'use strict';

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

// 抖音云容器服务要求容器内监听 8000；若平台注入了 PORT 环境变量则优先用它
const PORT = Number(process.env.PORT) || 8000;
const MAX_PLAYERS = 2;
const HEARTBEAT_MS = 30000;
// 单帧上限 64KB：联机同步的 payload 都是小对象，ws 默认允许 100MB，
// 公网端口上不加限制的话，一个恶意大帧就能把容器内存吃掉
const MAX_PAYLOAD = 64 * 1024;

/** roomId(string) -> Set<WebSocket> */
const rooms = new Map();
let clientSeq = 0;

/* ------------------------------------------------------------------ *
 * 工具
 * ------------------------------------------------------------------ */

/** 安全发送：连接已关闭时静默跳过，避免 send 抛异常打断整个流程 */
function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function sendError(ws, code, message, extra) {
  send(ws, Object.assign({ type: 'error', code, message }, extra || {}));
}

/** 生成 6 位数字房间号，且保证当前没被占用 */
function genRoomId() {
  for (let i = 0; i < 50; i += 1) {
    const id = String(Math.floor(100000 + Math.random() * 900000));
    if (!rooms.has(id)) return id;
  }
  // 极端情况（同时在线的房间数已接近 90 万）兜底：顺序找第一个空位
  for (let n = 100000; n <= 999999; n += 1) {
    const id = String(n);
    if (!rooms.has(id)) return id;
  }
  return null;
}

/** 客户端离开当前房间（断线、换房、创建新房前都会走这里） */
function leaveRoom(ws) {
  const roomId = ws.roomId;
  if (!roomId) return;
  ws.roomId = null;

  const set = rooms.get(roomId);
  if (!set) return;

  set.delete(ws);
  if (set.size === 0) {
    rooms.delete(roomId);
    return;
  }
  // 房间里还有人 → 通知对手「人走了」，让客户端可以退回等待页
  for (const peer of set) {
    send(peer, { type: 'peer_left', roomId });
  }
}

/* ------------------------------------------------------------------ *
 * 消息处理
 * ------------------------------------------------------------------ */

function handleMessage(ws, raw) {
  let msg;
  try {
    msg = JSON.parse(raw);
  } catch (e) {
    sendError(ws, 'BAD_JSON', '消息不是合法 JSON');
    return;
  }
  if (!msg || typeof msg !== 'object') {
    sendError(ws, 'BAD_MESSAGE', '消息必须是 JSON 对象');
    return;
  }

  switch (msg.type) {
    /* ---------------- 创建房间 ---------------- */
    case 'create_room': {
      leaveRoom(ws); // 已在房间里则先退出，一个连接只属于一个房间
      const roomId = genRoomId();
      if (!roomId) {
        sendError(ws, 'NO_ROOM_AVAILABLE', '房间号已用尽，请稍后再试');
        return;
      }
      rooms.set(roomId, new Set([ws]));
      ws.roomId = roomId;
      send(ws, { type: 'room_created', roomId, clientId: ws.clientId, count: 1 });
      break;
    }

    /* ---------------- 加入房间 ---------------- */
    case 'join_room': {
      const roomId = String(msg.roomId == null ? '' : msg.roomId).trim();

      if (!/^\d{6}$/.test(roomId)) {
        sendError(ws, 'BAD_ROOM_ID', '房间号必须是 6 位数字');
        return;
      }

      const set = rooms.get(roomId);
      if (!set) {
        sendError(ws, 'ROOM_NOT_FOUND', '房间不存在或已解散', { roomId });
        return;
      }

      // 重复加入同一房间：幂等返回，不报错
      if (set.has(ws)) {
        send(ws, {
          type: 'joined',
          roomId,
          clientId: ws.clientId,
          count: set.size,
          peerReady: set.size >= MAX_PLAYERS,
        });
        return;
      }

      if (set.size >= MAX_PLAYERS) {
        sendError(ws, 'ROOM_FULL', '房间已满（最多 2 人）', { roomId });
        return;
      }

      leaveRoom(ws); // 换房前先离开旧房间
      set.add(ws);
      ws.roomId = roomId;

      const count = set.size;
      send(ws, {
        type: 'joined',
        roomId,
        clientId: ws.clientId,
        count,
        peerReady: count >= MAX_PLAYERS,
      });
      // 通知房里已有的人：对手进来了
      for (const peer of set) {
        if (peer !== ws) send(peer, { type: 'peer_joined', roomId, count });
      }
      break;
    }

    /* ---------------- 转发 ---------------- */
    case 'relay': {
      const roomId = ws.roomId;
      if (!roomId) {
        sendError(ws, 'NOT_IN_ROOM', '请先创建或加入房间');
        return;
      }
      // 消息里带了 roomId 就校验，防止一个连接往别的房间投递
      if (msg.roomId != null && String(msg.roomId) !== roomId) {
        sendError(ws, 'ROOM_MISMATCH', '你现在不在该房间', { roomId: String(msg.roomId) });
        return;
      }

      const set = rooms.get(roomId);
      if (!set) {
        sendError(ws, 'ROOM_NOT_FOUND', '房间不存在或已解散', { roomId });
        return;
      }

      let delivered = 0;
      for (const peer of set) {
        if (peer === ws) continue;
        // payload 原样转发，不解析、不加工
        send(peer, { type: 'relay', payload: msg.payload });
        delivered += 1;
      }

      if (delivered === 0) {
        sendError(ws, 'NO_PEER', '对手还没进来，消息暂无处转发', { roomId });
      }
      break;
    }

    /* ---------------- 应用层心跳 ---------------- */
    case 'ping':
      send(ws, { type: 'pong', t: Date.now() });
      break;

    default:
      sendError(ws, 'UNKNOWN_TYPE', '未知消息类型: ' + String(msg.type));
  }
}

/* ------------------------------------------------------------------ *
 * HTTP 服务（健康检查 + WebSocket 升级共用 8000 端口）
 * ------------------------------------------------------------------ */

/** http server 先建（wss 要靠它做 upgrade），用 let 是为了让健康检查能安全引用 */
let wss = null;

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];

  if (url === '/' || url === '/health' || url === '/healthz') {
    const clients = wss ? wss.clients.size : 0;
    res.writeHead(200, {
      'Content-Type': 'application/json; charset=utf-8',
      'Access-Control-Allow-Origin': '*',
    });
    res.end(
      JSON.stringify({
        ok: true,
        service: 'couple-games-ws',
        rooms: rooms.size,
        clients,
        uptime: Math.round(process.uptime()),
      })
    );
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not Found');
});

// 不限制 path：客户端连 ws://host:8000/ 或 /ws 都能用
wss = new WebSocketServer({ server, maxPayload: MAX_PAYLOAD });

wss.on('connection', (ws, req) => {
  clientSeq += 1;
  ws.clientId = 'c' + clientSeq;
  ws.roomId = null;
  ws.isAlive = true;

  console.log('[conn] %s online=%d ip=%s', ws.clientId, wss.clients.size, (req.socket.remoteAddress || '-'));

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (data) => {
    // ws 在文本帧下给的是 Buffer，统一转字符串
    handleMessage(ws, data.toString());
  });

  ws.on('close', () => {
    leaveRoom(ws);
    console.log('[close] %s rooms=%d online=%d', ws.clientId, rooms.size, wss.clients.size);
  });

  ws.on('error', (err) => {
    console.warn('[error] %s %s', ws.clientId, err && err.message);
  });
});

/* 探活：30 秒一轮，上一轮没回 pong 的直接 terminate（close 事件会触发清理） */
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    try {
      ws.ping();
    } catch (e) {
      /* 忽略单次 ping 失败 */
    }
  }
}, HEARTBEAT_MS);

wss.on('close', () => clearInterval(heartbeat));

/* ------------------------------------------------------------------ *
 * 启动 / 优雅退出
 * ------------------------------------------------------------------ */

server.listen(PORT, '0.0.0.0', () => {
  console.log('WebSocket server listening on 0.0.0.0:%d', PORT);
  console.log('health check: GET http://127.0.0.1:%d/', PORT);
});

server.on('error', (err) => {
  console.error('server error:', err);
  process.exit(1);
});

// 兜底日志：异步回调里漏掉的 reject 不应该让进程静默挂掉、也不该刷屏整屏堆栈
process.on('unhandledRejection', (reason) => {
  console.warn('[unhandledRejection] %s', reason && reason.stack ? reason.stack : reason);
});

function shutdown(signal) {
  console.log('[shutdown] %s', signal);
  clearInterval(heartbeat);
  for (const ws of wss.clients) ws.close(1001, 'server shutdown');
  wss.close(() => {
    server.close(() => process.exit(0));
  });
  // 兜底：5 秒内没退干净就强退，避免容器卡在 stopping
  setTimeout(() => process.exit(0), 5000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
