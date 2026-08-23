import Fastify from 'fastify';
import websocket from '@fastify/websocket';
import fastifyStatic from '@fastify/static';

import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export async function createApp({
  port,
  onBrowserMessage,
  onNodeMessage,
  onNodeConnect,
}) {
  const app = Fastify({
    logger: false,
  });

  await app.register(websocket);

  await app.register(fastifyStatic, {
    root: path.join(
      __dirname,
      '../public'
    ),
  });

  const browserClients = new Set();
  const nodeClients = new Set();

  /*
   * Browser -> HTTP
   */
  app.get('/', async (_, reply) => {
    return reply.sendFile(
      'index.html'
    );
  });

  /*
   * Browser <-> Node
   */
  app.get(
    '/browser',
    {
      websocket: true,
    },
    (socket) => {
      browserClients.add(socket);

      console.log(
        '[app] browser connected'
      );

      socket.on('message', (message) => {
        onBrowserMessage(message);
      });

      socket.on('close', () => {
        browserClients.delete(socket);
      });
    }
  );

  /*
   * Node <-> Node
   */
  app.get(
    '/node',
    {
      websocket: true,
    },
    (socket) => {
      nodeClients.add(socket);

      console.log(
        '[app] node connected'
      );

      onNodeConnect?.(socket);

      socket.on('message', (message) => {
        onNodeMessage(
          message,
          socket
        );
      });

      socket.on('close', () => {
        nodeClients.delete(socket);
      });
    }
  );

  /*
   * Запускаем HTTP.
   */
  await app.listen({
    host: '0.0.0.0',
    port,
  });

  return {
    sendToBrowsers(message) {
      const data = JSON.stringify(message);

      for (const socket of browserClients) {
        if (socket.readyState === 1) {
          socket.send(data);
        }
      }
    },

    sendToNodes(message) {
      const data = JSON.stringify(message);

      for (const socket of nodeClients) {
        if (socket.readyState === 1) {
          socket.send(data);
        }
      }
    },

    getNodeClients() {
      return nodeClients;
    },

    async close() {
      for (const socket of browserClients) {
        socket.close();
      }

      for (const socket of nodeClients) {
        socket.close();
      }

      await app.close();
    },
  };
}