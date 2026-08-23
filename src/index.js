import WebSocket from 'ws';

import { Discovery } from './discovery.js';
import { createApp } from './app.js';

const PORT = 3333;

const discovery = new Discovery();

const incomingFiles = new Map();

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

function handleBrowserMessage(
  message,
  isBinary,
  sourceSocket
) {
  /*
   * Binary = данные файла.
   */
  if (isBinary) {
    handleBrowserBinary(
      message,
      sourceSocket
    );

    return;
  }

  let data;

  try {
    data = JSON.parse(
      message.toString()
    );
  } catch {
    return;
  }

  if (data.type === 'CHAT_MESSAGE') {
    if (isLeader()) {
      handleServerMessage(data);
    } else {
      sendToLeader(data);
    }

    return;
  }

  if (data.type === 'FILE_START') {
    if (isLeader()) {
      handleFileStart(data);
    } else {
      sendToLeader(data);
    }
  }
}

function handleFileStart(data) {
  incomingFiles.set(data.fileId, {
    ...data,
    source: null
  });

  /*
   * Передаём metadata всем клиентам.
   */
  broadcastToBrowsersAndNodes(data);
}

function broadcastToBrowsersAndNodes(
  message
) {
  app.sendToBrowsers(message);

  app.sendToNodes(message);
}

function handleBrowserBinary(
  data
) {
  if (!isLeader()) {
    if (
      serverSocket?.readyState ===
      WebSocket.OPEN
    ) {
      serverSocket.send(data);
    }

    return;
  }

  app.sendToNodesBinary(data);

  app.sendToBrowsersBinary(data);
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

function handleNodeMessage(
  message,
  isBinary,
  sourceSocket
) {
  /*
   * Binary = файл.
   */
  if (isBinary) {
    if (!isLeader()) {
      return;
    }

    /*
     * Отправляем всем Node-клиентам.
     */
    app.sendToNodesBinary(message);

    /*
     * Отправляем локальному браузеру.
     */
    app.sendToBrowsersBinary(message);

    return;
  }

  let data;

  try {
    data = JSON.parse(
      message.toString()
    );
  } catch {
    return;
  }

  if (data.type === 'CHAT_MESSAGE') {
    if (isLeader()) {
      app.sendToBrowsers(data);
      app.sendToNodes(data);
    }

    return;
  }

  if (data.type === 'FILE_START') {
    if (isLeader()) {
      app.sendToBrowsers(data);
      app.sendToNodes(data);
    }
  }
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

  socket.on(
    'message',
    (message, isBinary) => {
      if (isBinary) {
        app.sendToBrowsersBinary(
          message
        );

        return;
      }

      let data;

      try {
        data = JSON.parse(
          message.toString()
        );
      } catch {
        return;
      }

      if (data.type === 'CHAT_MESSAGE') {
        app.sendToBrowsers(data);
      }

      if (data.type === 'FILE_START') {
        app.sendToBrowsers(data);
      }
    }
  );

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
  sendToBrowsersBinary(data) {
    for (const socket of browserClients) {
      if (socket.readyState === 1) {
        socket.send(data);
      }
    }
  },
  sendToNodesBinary(data) {
    for (const socket of nodeClients) {
      if (socket.readyState === 1) {
        socket.send(data);
      }
    }
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