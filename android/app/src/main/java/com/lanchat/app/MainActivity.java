package com.lanchat.app;

import android.Manifest;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.os.Build;
import android.os.Bundle;
import android.util.Log;

import androidx.core.app.ActivityCompat;
import androidx.core.content.ContextCompat;

import com.getcapacitor.BridgeActivity;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Starts the chat server as a foreground service (ChatForegroundService, a
 * Java port of the original Node.js server) and points the Capacitor
 * WebView at it once it is listening.
 */
public class MainActivity extends BridgeActivity {

    private static final String TAG = "LanChat";
    private static final int NOTIFICATION_PERMISSION_REQUEST = 1;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        requestNotificationPermissionIfNeeded();

        ContextCompat.startForegroundService(this, new Intent(this, ChatForegroundService.class));

        waitForServerThenLoad();
    }

    private void requestNotificationPermissionIfNeeded() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU
                && ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS)
                        != PackageManager.PERMISSION_GRANTED) {
            ActivityCompat.requestPermissions(
                    this,
                    new String[] {Manifest.permission.POST_NOTIFICATIONS},
                    NOTIFICATION_PERMISSION_REQUEST
            );
        }
    }

    private void waitForServerThenLoad() {
        new Thread(() -> {
            for (int i = 0; i < 60; i++) {
                if (isPortOpen("127.0.0.1", ChatApp.PORT, 500)) {
                    Log.i(TAG, "Chat server is up, loading WebView");
                    runOnUiThread(() -> {
                        if (getBridge() != null && getBridge().getWebView() != null) {
                            getBridge().getWebView().loadUrl("http://localhost:" + ChatApp.PORT + "/");
                        }
                    });
                    return;
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignored) {
                }
            }
            Log.e(TAG, "Chat server did not come up on port " + ChatApp.PORT);
        }).start();
    }

    private boolean isPortOpen(String host, int port, int timeoutMs) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
