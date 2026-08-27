import dgram from 'node:dgram';
import os from 'node:os';
import crypto from 'node:crypto';

const DISCOVERY_PORT = 41234;
const SERVER_TIMEOUT = 6000;
const ANNOUNCE_INTERVAL = 2000;

export class Discovery {
  constructor() {
    this.nodeId = crypto.randomUUID();
    this.startedAt = Date.now();

    this.socket = dgram.createSocket('udp4');

    this.servers = new Map();

    this.changeListeners = new Set();
  }

  start() {
    this.socket.on('error', (error) => {
      console.error('[discovery] UDP error:', error);
    });

    this.socket.on('message', (message, rinfo) => {
      this.handleMessage(message, rinfo);
    });

    this.socket.bind(DISCOVERY_PORT, () => {
      this.socket.setBroadcast(true);

      console.log(`[discovery] listening on UDP ${DISCOVERY_PORT}`);

      this.startCleanup();
    });
  }

  handleMessage(message, rinfo) {
    let data;

    try {
      data = JSON.parse(message.toString());
    } catch {
      return;
    }

    if (data.type !== 'CHAT_SERVER') {
      return;
    }

    // Не обрабатываем собственное объявление.
    if (data.id === this.nodeId) {
      return;
    }

    const isNewServer = !this.servers.has(data.id);

    const server = {
      id: data.id,
      startedAt: data.startedAt,
      host: rinfo.address,
      port: data.port,
      lastSeen: Date.now(),
    };

    this.servers.set(server.id, server);

    if (isNewServer) {
      console.log(`[discovery] found server ${server.id} at ${server.host}:${server.port}`);
    }

    this.emitChange();
  }

  startCleanup() {
    setInterval(() => {
      const now = Date.now();

      for (const [id, server] of this.servers) {
        if (now - server.lastSeen > SERVER_TIMEOUT) {
          this.servers.delete(id);

          console.log(`[discovery] server ${id} disappeared`);
        }
      }

      this.emitChange();
    }, 1000);
  }

  /**
   * Возвращает лучшего кандидата.
   *
   * Сначала сравниваем время запуска.
   * Если оно одинаковое — сравниваем ID.
   */
  getBestServer() {
    const servers = [...this.servers.values()];

    if (servers.length === 0) {
      return null;
    }

    servers.sort((a, b) => {
      if (a.startedAt !== b.startedAt) {
        return a.startedAt - b.startedAt;
      }

      return a.id.localeCompare(b.id);
    });

    return servers[0];
  }

  /**
   * Подписка на изменение списка серверов.
   */
  onChange(callback) {
    this.changeListeners.add(callback);

    return () => {
      this.changeListeners.delete(callback);
    };
  }

  emitChange() {
    const server = this.getBestServer();

    for (const callback of this.changeListeners) {
      callback(server);
    }
  }

  /**
   * Объявляем себя сервером.
   */
  startAnnouncing(port) {
    const announce = () => {
      const message = Buffer.from(
        JSON.stringify({
          type: 'CHAT_SERVER',
          id: this.nodeId,
          startedAt: this.startedAt,
          port,
        }),
      );

      for (const address of this.getBroadcastAddresses()) {
        this.socket.send(message, 0, message.length, DISCOVERY_PORT, address);
      }
    };

    announce();

    const timer = setInterval(announce, ANNOUNCE_INTERVAL);

    return () => {
      clearInterval(timer);
    };
  }

  getBroadcastAddresses() {
    const result = [];

    const interfaces = os.networkInterfaces();

    for (const networkInterface of Object.values(interfaces)) {
      for (const address of networkInterface ?? []) {
        if (address.family !== 'IPv4' || address.internal) {
          continue;
        }

        const ip = address.address.split('.').map(Number);

        const mask = address.netmask.split('.').map(Number);

        const broadcast = ip.map((part, index) => part | (~mask[index] & 255));

        result.push(broadcast.join('.'));
      }
    }

    return result;
  }

  close() {
    this.socket.close();
  }
}
