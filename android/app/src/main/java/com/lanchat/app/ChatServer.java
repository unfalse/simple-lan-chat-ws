package com.lanchat.app;

import android.content.Context;
import android.content.res.AssetManager;
import android.util.Log;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import fi.iki.elonen.NanoHTTPD;
import fi.iki.elonen.NanoWSD;

/**
 * Java port of src/app.js: serves the chat page over HTTP and hosts two
 * websocket endpoints, "/browser" (local UI) and "/node" (other peers on
 * the LAN), mirroring the Fastify server's behavior exactly.
 */
public class ChatServer extends NanoWSD {

    private static final String TAG = "LanChat";

    public interface BrowserMessageListener {
        void onMessage(String text, byte[] binary, boolean isBinary, WebSocket source);
    }

    public interface NodeMessageListener {
        void onMessage(String text, byte[] binary, boolean isBinary, WebSocket source);
    }

    public interface NodeConnectListener {
        void onConnect(WebSocket socket);
    }

    private final Context context;
    private final Set<WebSocket> browserClients = new CopyOnWriteArraySet<>();
    private final Set<WebSocket> nodeClients = new CopyOnWriteArraySet<>();

    private BrowserMessageListener browserMessageListener;
    private NodeMessageListener nodeMessageListener;
    private NodeConnectListener nodeConnectListener;

    public ChatServer(Context context, int port) {
        super(port);
        this.context = context;
    }

    public void setBrowserMessageListener(BrowserMessageListener listener) {
        this.browserMessageListener = listener;
    }

    public void setNodeMessageListener(NodeMessageListener listener) {
        this.nodeMessageListener = listener;
    }

    public void setNodeConnectListener(NodeConnectListener listener) {
        this.nodeConnectListener = listener;
    }

    @Override
    protected Response serveHttp(IHTTPSession session) {
        String uri = session.getUri();
        if ("/".equals(uri)) {
            uri = "/index.html";
        }

        try (InputStream in = context.getAssets().open("public" + uri)) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] chunk = new byte[4096];
            int read;
            while ((read = in.read(chunk)) != -1) {
                buffer.write(chunk, 0, read);
            }
            byte[] bytes = buffer.toByteArray();
            String mime = guessMimeType(uri);
            return newFixedLengthResponse(Response.Status.OK, mime, new java.io.ByteArrayInputStream(bytes), bytes.length);
        } catch (IOException e) {
            return newFixedLengthResponse(Response.Status.NOT_FOUND, NanoHTTPD.MIME_PLAINTEXT, "Not found");
        }
    }

    private String guessMimeType(String uri) {
        String guessed = URLConnection.guessContentTypeFromName(uri);
        if (guessed != null) {
            return guessed;
        }
        if (uri.endsWith(".js")) {
            return "application/javascript";
        }
        if (uri.endsWith(".css")) {
            return "text/css";
        }
        return "application/octet-stream";
    }

    @Override
    protected WebSocket openWebSocket(IHTTPSession handshake) {
        boolean isNode = "/node".equals(handshake.getUri());
        return new ChatSocket(handshake, isNode);
    }

    public void sendToBrowsers(String json) {
        for (WebSocket socket : browserClients) {
            trySend(socket, json);
        }
    }

    public void sendToBrowsersBinary(byte[] data) {
        for (WebSocket socket : browserClients) {
            trySendBinary(socket, data);
        }
    }

    public void sendToNodes(String json) {
        for (WebSocket socket : nodeClients) {
            trySend(socket, json);
        }
    }

    public void sendToNodesBinary(byte[] data) {
        for (WebSocket socket : nodeClients) {
            trySendBinary(socket, data);
        }
    }

    private void trySend(WebSocket socket, String json) {
        try {
            socket.send(json);
        } catch (IOException ignored) {
        }
    }

    private void trySendBinary(WebSocket socket, byte[] data) {
        try {
            socket.send(data);
        } catch (IOException ignored) {
        }
    }

    private class ChatSocket extends WebSocket {

        private final boolean isNode;

        ChatSocket(IHTTPSession handshake, boolean isNode) {
            super(handshake);
            this.isNode = isNode;
        }

        @Override
        protected void onOpen() {
            if (isNode) {
                nodeClients.add(this);
                Log.i(TAG, "[app] node connected");
                if (nodeConnectListener != null) {
                    nodeConnectListener.onConnect(this);
                }
            } else {
                browserClients.add(this);
                Log.i(TAG, "[app] browser connected");
            }
        }

        @Override
        protected void onClose(NanoWSD.WebSocketFrame.CloseCode code, String reason, boolean initiatedByRemote) {
            browserClients.remove(this);
            nodeClients.remove(this);
        }

        @Override
        protected void onMessage(NanoWSD.WebSocketFrame frame) {
            boolean isBinary = frame.getOpCode() == NanoWSD.WebSocketFrame.OpCode.Binary;
            String text = null;
            byte[] binary = null;
            if (isBinary) {
                binary = frame.getBinaryPayload();
            } else {
                try {
                    text = frame.getTextPayload();
                } catch (Exception e) {
                    text = safeDecode(frame.getBinaryPayload());
                }
            }

            if (isNode) {
                if (nodeMessageListener != null) {
                    nodeMessageListener.onMessage(text, binary, isBinary, this);
                }
            } else {
                if (browserMessageListener != null) {
                    browserMessageListener.onMessage(text, binary, isBinary, this);
                }
            }
        }

        @Override
        protected void onPong(NanoWSD.WebSocketFrame pong) {
        }

        @Override
        protected void onException(IOException exception) {
            Log.e(TAG, "[app] websocket error", exception);
        }

        private String safeDecode(byte[] bytes) {
            try {
                return new String(bytes, StandardCharsets.UTF_8);
            } catch (Exception e) {
                return null;
            }
        }
    }
}
