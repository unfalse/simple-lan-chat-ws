package com.lanchat.app;

import android.content.Context;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import fi.iki.elonen.NanoWSD;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;

/**
 * Java port of src/index.js: routes chat/file messages between the local
 * browser (WebView), this node's role as leader/follower, and other nodes
 * on the LAN, using {@link Discovery} for leader election.
 */
public class ChatApp {

    private static final String TAG = "LanChat";
    public static final int PORT = 3333;

    private final Discovery discovery = new Discovery();
    private final OkHttpClient httpClient = new OkHttpClient();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private ChatServer server;
    private volatile String role = "client";
    private volatile Discovery.ServerInfo currentServer;
    private volatile WebSocket serverSocket;
    private ScheduledFuture<?> electionTimer;

    public void start(Context context) {
        server = new ChatServer(context, PORT);
        server.setBrowserMessageListener(this::handleBrowserMessage);
        server.setNodeMessageListener(this::handleNodeMessage);
        server.setNodeConnectListener(socket -> Log.i(TAG, "[server] node client connected"));

        try {
            /*
             * 0 = без таймаута чтения сокета.
             * WebSocket-соединение может подолгу
             * молчать (пока пользователь печатает
             * или выбирает файл), и NanoHTTPD иначе
             * рвёт его по SocketTimeoutException.
             */
            server.start(0, false);
            Log.i(TAG, "[app] listening on " + PORT);
        } catch (IOException e) {
            Log.e(TAG, "[app] failed to start HTTP/WS server", e);
            return;
        }

        discovery.onChange(this::handleServerChange);
        discovery.start();
        scheduleElection();
    }

    public void stop() {
        if (electionTimer != null) {
            electionTimer.cancel(false);
        }
        discovery.close();
        if (serverSocket != null) {
            serverSocket.close(1000, null);
        }
        if (server != null) {
            server.stop();
        }
        executor.shutdownNow();
    }

    private boolean isLeader() {
        return "server".equals(role);
    }

    // --------------------------------------------------
    // Browser message
    // --------------------------------------------------

    private void handleBrowserMessage(String text, byte[] binary, boolean isBinary, NanoWSD.WebSocket source) {
        if (isBinary) {
            handleBrowserBinary(binary);
            return;
        }

        JSONObject data = parse(text);
        if (data == null) {
            return;
        }

        String type = data.optString("type");
        if ("CHAT_MESSAGE".equals(type)) {
            if (isLeader()) {
                handleServerMessage(text);
            } else {
                sendToLeader(text);
            }
        } else if ("FILE_START".equals(type) || "FILE_END".equals(type)) {
            if (isLeader()) {
                broadcastToBrowsersAndNodes(text);
            } else {
                sendToLeader(text);
            }
        }
    }

    private void broadcastToBrowsersAndNodes(String json) {
        server.sendToBrowsers(json);
        server.sendToNodes(json);
    }

    private void handleBrowserBinary(byte[] data) {
        if (!isLeader()) {
            if (serverSocket != null) {
                serverSocket.send(ByteString.of(data));
            }
            return;
        }
        server.sendToNodesBinary(data);
        server.sendToBrowsersBinary(data);
    }

    // --------------------------------------------------
    // Server message (leader received a CHAT_MESSAGE from its own browser)
    // --------------------------------------------------

    private void handleServerMessage(String json) {
        server.sendToBrowsers(json);
        server.sendToNodes(json);
    }

    // --------------------------------------------------
    // Node message (from another node connected to /node)
    // --------------------------------------------------

    private void handleNodeMessage(String text, byte[] binary, boolean isBinary, NanoWSD.WebSocket source) {
        if (isBinary) {
            if (!isLeader()) {
                return;
            }
            server.sendToNodesBinary(binary);
            server.sendToBrowsersBinary(binary);
            return;
        }

        JSONObject data = parse(text);
        if (data == null) {
            return;
        }

        String type = data.optString("type");
        if (("CHAT_MESSAGE".equals(type) || "FILE_START".equals(type) || "FILE_END".equals(type)) && isLeader()) {
            server.sendToBrowsers(text);
            server.sendToNodes(text);
        }
    }

    // --------------------------------------------------
    // Connecting to leader (as a follower)
    // --------------------------------------------------

    private void connectToServer(Discovery.ServerInfo server) {
        if (isLeader()) {
            return;
        }
        if (currentServer != null && currentServer.id.equals(server.id) && serverSocket != null) {
            return;
        }

        if (serverSocket != null) {
            serverSocket.close(1000, null);
        }
        currentServer = server;

        String url = "ws://" + server.host + ":" + server.port + "/node";
        Log.i(TAG, "[node] connecting to " + url);

        Request request = new Request.Builder().url(url).build();
        AtomicReference<WebSocket> socketRef = new AtomicReference<>();
        WebSocket socket = httpClient.newWebSocket(request, new WebSocketListener() {
            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                Log.i(TAG, "[node] connected to " + server.id);
            }

            @Override
            public void onMessage(WebSocket webSocket, String text) {
                JSONObject data = parse(text);
                if (data == null) {
                    return;
                }
                String type = data.optString("type");
                if ("CHAT_MESSAGE".equals(type) || "FILE_START".equals(type) || "FILE_END".equals(type)) {
                    ChatApp.this.server.sendToBrowsers(text);
                }
            }

            @Override
            public void onMessage(WebSocket webSocket, ByteString bytes) {
                ChatApp.this.server.sendToBrowsersBinary(bytes.toByteArray());
            }

            @Override
            public void onClosed(WebSocket webSocket, int code, String reason) {
                if (serverSocket == socketRef.get()) {
                    serverSocket = null;
                    currentServer = null;
                }
                Log.i(TAG, "[node] leader connection closed");
                scheduleElection();
            }

            @Override
            public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                if (serverSocket == socketRef.get()) {
                    serverSocket = null;
                    currentServer = null;
                }
                scheduleElection();
            }
        });
        socketRef.set(socket);
        serverSocket = socket;
    }

    private void sendToLeader(String json) {
        if (serverSocket != null) {
            serverSocket.send(json);
        }
    }

    // --------------------------------------------------
    // Discovery changed
    // --------------------------------------------------

    private void handleServerChange(Discovery.ServerInfo bestServer) {
        if (isLeader()) {
            /*
             * Мы уже стали лидером, но, возможно,
             * объявление конкурента просто пришло
             * с опозданием (split-brain). Если он
             * по факту приоритетнее нас — уступаем.
             */
            if (bestServer != null && shouldStepDownFor(bestServer)) {
                stepDown(bestServer);
            }
            return;
        }
        if (bestServer == null) {
            scheduleElection();
            return;
        }
        if (currentServer == null || !currentServer.id.equals(bestServer.id)) {
            connectToServer(bestServer);
        }
    }

    private boolean shouldStepDownFor(Discovery.ServerInfo server) {
        if (server.startedAt != discovery.getStartedAt()) {
            return server.startedAt < discovery.getStartedAt();
        }
        return server.id.compareTo(discovery.getNodeId()) < 0;
    }

    private void stepDown(Discovery.ServerInfo server) {
        Log.i(TAG, "[election] stepping down, " + server.id + " is the real leader");
        discovery.stopAnnouncing();
        role = "client";
        connectToServer(server);
    }

    // --------------------------------------------------
    // Election
    // --------------------------------------------------

    private synchronized void scheduleElection() {
        if (electionTimer != null && !electionTimer.isDone()) {
            return;
        }
        long delay = 1000 + (long) (Math.random() * 2000);
        electionTimer = executor.schedule(this::runElection, delay, TimeUnit.MILLISECONDS);
    }

    private void runElection() {
        if (isLeader()) {
            return;
        }
        Discovery.ServerInfo best = discovery.getBestServer();
        if (best != null) {
            connectToServer(best);
            return;
        }
        becomeLeader();
    }

    private void becomeLeader() {
        if (isLeader()) {
            return;
        }
        Discovery.ServerInfo best = discovery.getBestServer();
        if (best != null) {
            connectToServer(best);
            return;
        }

        Log.i(TAG, "[election] becoming leader");
        role = "server";
        currentServer = null;
        if (serverSocket != null) {
            serverSocket.close(1000, null);
            serverSocket = null;
        }
        discovery.startAnnouncing(PORT);
        Log.i(TAG, "[server] I am the leader");
    }

    private JSONObject parse(String text) {
        if (text == null) {
            return null;
        }
        try {
            return new JSONObject(text);
        } catch (JSONException e) {
            return null;
        }
    }
}
