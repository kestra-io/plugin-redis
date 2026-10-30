package io.kestra.plugin.redis.stream;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

// A TCP proxy in front of a real Redis that forwards everything except one command, which it reads but never answers,
// so a client call to that command hangs like it would against a stuck server until the connection is closed.
final class StallingProxy implements AutoCloseable {
    private final ServerSocket serverSocket;
    private final String backendHost;
    private final int backendPort;
    private final String stalledCommand;
    private final CountDownLatch stalled = new CountDownLatch(1);

    private StallingProxy(ServerSocket serverSocket, String backendHost, int backendPort, String stalledCommand) {
        this.serverSocket = serverSocket;
        this.backendHost = backendHost;
        this.backendPort = backendPort;
        this.stalledCommand = stalledCommand;

        var acceptThread = new Thread(this::acceptLoop, "redis-stalling-proxy-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    static StallingProxy start(String backendHost, int backendPort, String stalledCommand) throws IOException {
        return new StallingProxy(new ServerSocket(0, 50, InetAddress.getLoopbackAddress()), backendHost, backendPort, stalledCommand);
    }

    int getPort() {
        return serverSocket.getLocalPort();
    }

    boolean awaitStalled(Duration timeout) throws InterruptedException {
        return stalled.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void acceptLoop() {
        try {
            while (!serverSocket.isClosed()) {
                var client = serverSocket.accept();
                var clientThread = new Thread(() -> handleClient(client), "redis-stalling-proxy-client");
                clientThread.setDaemon(true);
                clientThread.start();
            }
        } catch (IOException ignored) {
            // server socket closed: the proxy is shutting down
        }
    }

    private void handleClient(Socket client) {
        try (client; var backend = new Socket(backendHost, backendPort)) {
            var backendToClient = new Thread(() -> pipe(backend, client), "redis-stalling-proxy-b2c");
            backendToClient.setDaemon(true);
            backendToClient.start();

            var in = client.getInputStream();
            var out = backend.getOutputStream();
            byte[] command;
            while ((command = readCommand(in)) != null) {
                if (isStalled(command)) {
                    stalled.countDown();
                } else {
                    out.write(command);
                    out.flush();
                }
            }
        } catch (IOException ignored) {
            // the client or the backend closed the connection, for example when a kill tears the socket down
        }
    }

    private static void pipe(Socket from, Socket to) {
        try (from; to) {
            from.getInputStream().transferTo(to.getOutputStream());
        } catch (IOException ignored) {
            // connection closed
        }
    }

    // reads one RESP array of bulk strings, the format every client sends requests in, and returns its raw bytes
    private static byte[] readCommand(InputStream in) throws IOException {
        var raw = new ByteArrayOutputStream();
        var arity = readHeader(in, raw, '*');
        if (arity == null) {
            return null;
        }
        for (int i = 0; i < arity; i++) {
            var length = readHeader(in, raw, '$');
            if (length == null) {
                return null;
            }
            var payload = in.readNBytes(length + 2);
            if (payload.length != length + 2) {
                throw new EOFException("Unexpected end of stream while reading a RESP payload");
            }
            raw.write(payload);
        }
        return raw.toByteArray();
    }

    private static Integer readHeader(InputStream in, ByteArrayOutputStream raw, char prefix) throws IOException {
        var first = in.read();
        if (first == -1) {
            return null;
        }
        raw.write(first);
        if (first != prefix) {
            throw new IOException("Unexpected RESP prefix: " + (char) first);
        }

        var digits = new StringBuilder();
        int b;
        while ((b = in.read()) != -1 && b != '\r') {
            raw.write(b);
            digits.append((char) b);
        }
        if (b == -1) {
            return null;
        }
        raw.write(b);

        var lf = in.read();
        if (lf == -1) {
            return null;
        }
        raw.write(lf);

        return Integer.parseInt(digits.toString());
    }

    private boolean isStalled(byte[] rawCommand) {
        var lines = new String(rawCommand, StandardCharsets.US_ASCII).split("\r\n");
        return lines.length > 2 && stalledCommand.equalsIgnoreCase(lines[2]);
    }

    @Override
    public void close() throws IOException {
        serverSocket.close();
    }
}
