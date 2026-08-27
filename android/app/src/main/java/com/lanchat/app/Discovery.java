package com.lanchat.app;

import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Java port of src/discovery.js: announces this node on the LAN over UDP
 * broadcast and tracks other announcing nodes so the app can elect a leader.
 */
public class Discovery {

    private static final String TAG = "LanChat";
    private static final int DISCOVERY_PORT = 41234;
    private static final long SERVER_TIMEOUT_MS = 6000;
    private static final long ANNOUNCE_INTERVAL_MS = 2000;

    public static class ServerInfo {
        public final String id;
        public final long startedAt;
        public final String host;
        public final int port;
        public long lastSeen;

        ServerInfo(String id, long startedAt, String host, int port, long lastSeen) {
            this.id = id;
            this.startedAt = startedAt;
            this.host = host;
            this.port = port;
            this.lastSeen = lastSeen;
        }
    }

    public interface ChangeListener {
        void onChange(ServerInfo bestServer);
    }

    private final String nodeId = UUID.randomUUID().toString();
    private final long startedAt = System.currentTimeMillis();

    private final Map<String, ServerInfo> servers = new ConcurrentHashMap<>();
    private final CopyOnWriteArraySet<ChangeListener> changeListeners = new CopyOnWriteArraySet<>();

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
    private DatagramSocket socket;
    private ScheduledFuture<?> announceFuture;

    public void start() {
        try {
            socket = new DatagramSocket(null);
            socket.setReuseAddress(true);
            socket.setBroadcast(true);
            socket.bind(new java.net.InetSocketAddress(DISCOVERY_PORT));
            Log.i(TAG, "[discovery] listening on UDP " + DISCOVERY_PORT);
        } catch (IOException e) {
            Log.e(TAG, "[discovery] failed to bind UDP socket", e);
            return;
        }

        executor.execute(this::receiveLoop);
        executor.scheduleWithFixedDelay(this::cleanup, 1, 1, TimeUnit.SECONDS);
    }

    private void receiveLoop() {
        byte[] buffer = new byte[2048];
        while (!socket.isClosed()) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
            } catch (IOException e) {
                if (socket.isClosed()) {
                    return;
                }
                continue;
            }
            handleMessage(packet);
        }
    }

    private void handleMessage(DatagramPacket packet) {
        String text = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
        JSONObject data;
        try {
            data = new JSONObject(text);
        } catch (JSONException e) {
            return;
        }

        if (!"CHAT_SERVER".equals(data.optString("type"))) {
            return;
        }

        String id = data.optString("id");
        if (nodeId.equals(id)) {
            return;
        }

        boolean isNewServer = !servers.containsKey(id);
        ServerInfo server = new ServerInfo(
                id,
                data.optLong("startedAt"),
                packet.getAddress().getHostAddress(),
                data.optInt("port"),
                System.currentTimeMillis()
        );
        servers.put(id, server);

        if (isNewServer) {
            Log.i(TAG, "[discovery] found server " + id + " at " + server.host + ":" + server.port);
        }

        emitChange();
    }

    private void cleanup() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, ServerInfo> entry : servers.entrySet()) {
            if (now - entry.getValue().lastSeen > SERVER_TIMEOUT_MS) {
                servers.remove(entry.getKey());
                Log.i(TAG, "[discovery] server " + entry.getKey() + " disappeared");
            }
        }
        emitChange();
    }

    public ServerInfo getBestServer() {
        List<ServerInfo> list = new ArrayList<>(servers.values());
        if (list.isEmpty()) {
            return null;
        }
        Collections.sort(list, (a, b) -> {
            if (a.startedAt != b.startedAt) {
                return Long.compare(a.startedAt, b.startedAt);
            }
            return a.id.compareTo(b.id);
        });
        return list.get(0);
    }

    public void onChange(ChangeListener listener) {
        changeListeners.add(listener);
    }

    private void emitChange() {
        ServerInfo best = getBestServer();
        for (ChangeListener listener : changeListeners) {
            listener.onChange(best);
        }
    }

    public synchronized void startAnnouncing(int port) {
        if (announceFuture != null) {
            return;
        }
        announceFuture = executor.scheduleWithFixedDelay(() -> announce(port), 0, ANNOUNCE_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public synchronized void stopAnnouncing() {
        if (announceFuture != null) {
            announceFuture.cancel(false);
            announceFuture = null;
        }
    }

    private void announce(int port) {
        JSONObject message = new JSONObject();
        try {
            message.put("type", "CHAT_SERVER");
            message.put("id", nodeId);
            message.put("startedAt", startedAt);
            message.put("port", port);
        } catch (JSONException e) {
            return;
        }
        byte[] payload = message.toString().getBytes(StandardCharsets.UTF_8);

        for (InetAddress address : getBroadcastAddresses()) {
            try {
                socket.send(new DatagramPacket(payload, payload.length, address, DISCOVERY_PORT));
            } catch (IOException ignored) {
            }
        }
    }

    private List<InetAddress> getBroadcastAddresses() {
        List<InetAddress> result = new ArrayList<>();
        try {
            for (NetworkInterface networkInterface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!networkInterface.isUp() || networkInterface.isLoopback()) {
                    continue;
                }
                for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                    InetAddress broadcast = interfaceAddress.getBroadcast();
                    if (broadcast != null && interfaceAddress.getAddress() instanceof Inet4Address) {
                        result.add(broadcast);
                    }
                }
            }
        } catch (SocketException e) {
            Log.e(TAG, "[discovery] failed to enumerate network interfaces", e);
        }
        return result;
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getStartedAt() {
        return startedAt;
    }

    public void close() {
        stopAnnouncing();
        if (socket != null) {
            socket.close();
        }
        executor.shutdownNow();
    }
}
