package uk.sky.cirrus.util;

import java.io.IOException;
import java.net.ServerSocket;

public class PortScavenger {

    public static int getFreePort() {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            int port = socket.getLocalPort();
            closeSocket(socket);
            return port;
        } catch (IOException ignored) {
        } finally {
            closeSocket(socket);
        }
        throw new IllegalStateException("Could not find a free TCP/IP port to start embedded Jetty HTTP Server on");

    }

    private static void closeSocket(ServerSocket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }
}
