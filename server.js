const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

// ── HTTP server ──────────────────────────────────────────────
const server = http.createServer((req, res) => {
  let filePath = path.join(__dirname, req.url === '/' ? 'index.html' : req.url);
  const ext = path.extname(filePath);
  const mime = {
    '.html': 'text/html',
    '.js':   'application/javascript',
    '.css':  'text/css',
    '.json': 'application/json',
    '.png':  'image/png',
  };

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not found');
      return;
    }
    res.writeHead(200, { 'Content-Type': mime[ext] || 'text/plain' });
    res.end(data);
  });
});

// ── State ────────────────────────────────────────────────────
// queue: { [topic]: [ws, ws, ...] }
const queue = {};
// peers: Map<ws, ws>  (bidirectional)
const peers = new Map();
// meta: Map<ws, { name, topic, language }>
const meta  = new Map();

// ── WebSocket signaling ──────────────────────────────────────
const wss = new WebSocketServer({ server });

function broadcast(ws, msg) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}

wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      // ── Join waiting queue ───────────────────────────────
      case 'join': {
        const { name, topic, language, problem } = msg;
        meta.set(ws, { name, topic, language, problem });

        if (!queue[topic]) queue[topic] = [];

        // Find a waiting peer with compatible language preference
        const idx = queue[topic].findIndex(p => {
          const pm = meta.get(p);
          return pm && (pm.language === language || pm.language === 'any' || language === 'any');
        });

        if (idx !== -1) {
          // Match found
          const peer = queue[topic].splice(idx, 1)[0];
          peers.set(ws, peer);
          peers.set(peer, ws);

          const mySelf = meta.get(ws);
          const peerM  = meta.get(peer);

          broadcast(ws, {
            type: 'matched',
            peerName:     peerM.name,
            peerTopic:    peerM.topic,
            peerProblem:  peerM.problem || '',
            initiator:    false,
          });
          broadcast(peer, {
            type: 'matched',
            peerName:     mySelf.name,
            peerTopic:    mySelf.topic,
            peerProblem:  mySelf.problem || '',
            initiator:    true,
          });
        } else {
          // Add to queue
          queue[topic].push(ws);
          broadcast(ws, { type: 'waiting' });
        }
        break;
      }

      // ── WebRTC signaling ─────────────────────────────────
      case 'offer':
      case 'answer':
      case 'ice': {
        const peer = peers.get(ws);
        if (peer) broadcast(peer, msg);
        break;
      }

      // ── Hang up ──────────────────────────────────────────
      case 'hangup': {
        const peer = peers.get(ws);
        if (peer) {
          broadcast(peer, { type: 'peer_left' });
          peers.delete(peer);
        }
        peers.delete(ws);
        break;
      }

      // ── Cancel queue ─────────────────────────────────────
      case 'cancel': {
        const m = meta.get(ws);
        if (m && queue[m.topic]) {
          queue[m.topic] = queue[m.topic].filter(p => p !== ws);
        }
        break;
      }
    }
  });

  ws.on('close', () => {
    // Remove from queue
    const m = meta.get(ws);
    if (m && queue[m.topic]) {
      queue[m.topic] = queue[m.topic].filter(p => p !== ws);
    }
    // Notify peer
    const peer = peers.get(ws);
    if (peer) {
      broadcast(peer, { type: 'peer_left' });
      peers.delete(peer);
    }
    peers.delete(ws);
    meta.delete(ws);
  });
});

// ── Start ────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n  🌿 Samsaram | സംസാരം  running at http://localhost:${PORT}\n`);
  console.log('  Open that URL in TWO browser tabs (or two browsers) to test a call.\n');
});
