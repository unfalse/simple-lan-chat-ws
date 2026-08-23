import WebSocket from 'ws';

import { Discovery } from './discovery.js';
import { createApp } from './app.js';

const PORT = 3333;

const discovery = new Discovery();

let role = 'client';

let currentServer = null;
let serverSocket = null;

let stopAnnouncing = null;
let electionTimer = null;

function isLeader() {
  return role === 'server';
}

/*
 * --------------------------------------------------
 * Browser message
 * --------------------------------------------------
 */

function handleBrowserMessage(message) {
  let data;

  try {
    data = JSON.parse(
      message.toString()
    );
  } catch {
    return;
  }

  if (data.type !== 'CHAT_MESSAGE') {
    return;
  }

  if (isLeader()) {
    handleServerMessage(data);
  } else {
    sendToLeader(data);
  }
}

/*
 * --------------------------------------------------
 * Server message
 * --------------------------------------------------
 */

function handleServerMessage(message) {
  /*
   * Показываем сообщение
   * браузеру на сервере.
   */
  app.sendToBrowsers(message);

  /*
   * Отправляем остальным Node.js.
   */
  app.sendToNodes(message);
}

/*
 * --------------------------------------------------
 * Node message
 * --------------------------------------------------
 */

function handleNodeMessage(message, sourceSocket) {
  let data;

  try {
    data = JSON.parse(message.toString());
  } catch (error) {
    console.log('[server] invalid node message');

    return;
  }

  if (data.type !== 'CHAT_MESSAGE') {
    return;
  }

  /*
   * 1. Показываем сообщение браузеру
   *    на самом сервере.
   */
  app.sendToBrowsers(data);

  /*
   * 2. Отправляем сообщение всем Node-клиентам.
   */
  app.sendToNodes(data);
}

/*
 * --------------------------------------------------
 * Подключение к leader
 * --------------------------------------------------
 */

function connectToServer(server) {
  if (isLeader()) {
    return;
  }

  if (
    currentServer?.id === server.id &&
    serverSocket
  ) {
    return;
  }

  serverSocket?.close();

  currentServer = server;

  const url =
    `ws://${server.host}:${server.port}/node`;

  console.log(
    `[node] connecting to ${url}`
  );

  const socket =
    new WebSocket(url);

  serverSocket = socket;

  socket.on('open', () => {
    console.log(
      `[node] connected to ${server.id}`
    );
  });

  socket.on('message', (message) => {
    console.log(
      '[node] received from server:',
      message.toString()
    );

    let data;

    try {
      data = JSON.parse(
        message.toString()
      );
    } catch (error) {
      console.log(
        '[node] invalid server message'
      );

      return;
    }

    if (
      data.type === 'CHAT_MESSAGE'
    ) {
      app.sendToBrowsers(data);
    }
  });

  socket.on('close', () => {
    if (serverSocket === socket) {
      serverSocket = null;
      currentServer = null;
    }

    console.log(
      '[node] leader connection closed'
    );

    scheduleElection();
  });

  socket.on('error', () => { });
}

/*
 * --------------------------------------------------
 * Send to leader
 * --------------------------------------------------
 */

function sendToLeader(message) {
  if (
    serverSocket?.readyState ===
    WebSocket.OPEN
  ) {
    serverSocket.send(
      JSON.stringify(message)
    );
  }
}

/*
 * --------------------------------------------------
 * Discovery changed
 * --------------------------------------------------
 */

function handleServerChange(server) {
  if (isLeader()) {
    return;
  }

  if (!server) {
    console.log(
      '[discovery] no server found'
    );

    scheduleElection();

    return;
  }

  if (
    currentServer?.id !== server.id
  ) {
    connectToServer(server);
  }
}

/*
 * --------------------------------------------------
 * Election
 * --------------------------------------------------
 */

function scheduleElection() {
  if (electionTimer) {
    return;
  }

  const delay =
    1000 + Math.random() * 2000;

  console.log(
    `[election] waiting ${Math.round(delay)}ms`
  );

  electionTimer = setTimeout(() => {
    electionTimer = null;

    runElection();
  }, delay);
}

function runElection() {
  if (isLeader()) {
    return;
  }

  const server =
    discovery.getBestServer();

  if (server) {
    connectToServer(server);

    return;
  }

  becomeLeader();
}

/*
 * --------------------------------------------------
 * Become leader
 * --------------------------------------------------
 */

function becomeLeader() {
  if (isLeader()) {
    return;
  }

  /*
   * Последняя проверка.
   */
  const server =
    discovery.getBestServer();

  if (server) {
    connectToServer(server);

    return;
  }

  console.log(
    '[election] becoming leader'
  );

  role = 'server';

  currentServer = null;

  serverSocket?.close();
  serverSocket = null;

  /*
   * Теперь начинаем рекламировать
   * себя в LAN.
   */
  stopAnnouncing =
    discovery.startAnnouncing(
      PORT
    );

  console.log(
    '[server] I am the leader'
  );
}

/*
 * --------------------------------------------------
 * App
 * --------------------------------------------------
 */

const app = await createApp({
  port: PORT,

  onBrowserMessage:
    handleBrowserMessage,

  onNodeMessage:
    handleNodeMessage,

  onNodeConnect(socket) {
    console.log(
      '[server] node client connected'
    );
  },
});

/*
 * --------------------------------------------------
 * Discovery
 * --------------------------------------------------
 */

discovery.onChange(
  handleServerChange
);

discovery.start();

/*
 * Начинаем поиск лидера.
 */
scheduleElection();

/*
 * --------------------------------------------------
 * Shutdown
 * --------------------------------------------------
 */

process.on('SIGINT', async () => {
  console.log(
    '\nShutting down...'
  );

  stopAnnouncing?.();

  serverSocket?.close();

  discovery.close();

  await app.close();

  process.exit(0);
});