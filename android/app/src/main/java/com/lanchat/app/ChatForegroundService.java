package com.lanchat.app;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.Service;
import android.content.Intent;
import android.os.Build;
import android.os.IBinder;

import androidx.core.app.NotificationCompat;

/**
 * Runs the chat server (Discovery + HTTP/WebSocket relay) as a foreground
 * service, not as a plain background thread tied to the Activity. A plain
 * thread dies as soon as Android reclaims the app's process while it's
 * minimized (common on OEM battery managers like MIUI); a foreground
 * service with an active notification is kept at a much higher process
 * priority and survives backgrounding.
 */
public class ChatForegroundService extends Service {

    private static final String CHANNEL_ID = "lan_chat_server";
    private static final int NOTIFICATION_ID = 1;

    private static final ChatApp chatApp = new ChatApp();
    private static volatile boolean started = false;

    @Override
    public void onCreate() {
        super.onCreate();

        createNotificationChannel();
        startForeground(NOTIFICATION_ID, buildNotification());

        if (!started) {
            started = true;
            new Thread(() -> chatApp.start(getApplicationContext())).start();
        }
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        return START_STICKY;
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    @Override
    public void onDestroy() {
        super.onDestroy();

        chatApp.stop();
        started = false;
    }

    private void createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel channel = new NotificationChannel(
                    CHANNEL_ID,
                    "LAN Chat server",
                    NotificationManager.IMPORTANCE_LOW
            );

            NotificationManager manager = getSystemService(NotificationManager.class);
            manager.createNotificationChannel(channel);
        }
    }

    private Notification buildNotification() {
        return new NotificationCompat.Builder(this, CHANNEL_ID)
                .setContentTitle("LAN Chat")
                .setContentText("Сервер чата работает в фоне")
                .setSmallIcon(R.mipmap.ic_launcher)
                .setOngoing(true)
                .build();
    }
}
