package io.kestra.plugin.redis.list;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

class TriggerTest extends AbstractTriggerTest {
    @Override
    protected String getKey() {
        return "mytriggerkey_trigger";
    }

    @BeforeEach
    void setUp() throws Exception {
        push();
    }

    @Test
    @EvaluateTrigger(flow = "flows/trigger.yaml", triggerId = "watch")
    void run(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));
        Execution execution = optionalExecution.get();

        Integer count = (Integer) execution.getTrigger().getVariables().get("count");
        assertThat(count, greaterThanOrEqualTo(2));
    }

    @Test
    void shouldUnblockInFlightEvaluateOnKill() throws Exception {
        // A plain LPOP on an empty key returns immediately, so it can't be used to prove kill()
        // unblocks an in-flight call. Instead, route the trigger through a local proxy that
        // transparently forwards the connection handshake to the real Redis server (so evaluate()
        // opens a genuine, working connection) but silently swallows the LPOP request without ever
        // replying: the sync command is then truly stuck, and only kill() closing the connection
        // can free it.
        // Backend host/port mirror REDIS_URI ("redis://:redis@localhost:6379/0") from AbstractTriggerTest.
        try (BlockingLpopProxy proxy = BlockingLpopProxy.start("localhost", 6379)) {
            Trigger trigger = Trigger.builder()
                .id(TriggerTest.class.getSimpleName())
                .type(Trigger.class.getName())
                .url(Property.ofValue("redis://:redis@localhost:" + proxy.getPort() + "/0"))
                .key(Property.ofValue("mytriggerkey_kill_" + IdUtils.create()))
                .maxDuration(Property.ofValue(Duration.ofSeconds(30)))
                .build();

            RunContext runContext = runContextFactory.of(Map.of());
            ConditionContext conditionContext = ConditionContext.builder()
                .runContext(runContext)
                .build();

            var completed = new CountDownLatch(1);
            var thrown = new AtomicReference<Throwable>();
            var result = new AtomicReference<Optional<Execution>>();
            Thread runner = new Thread(() -> {
                try {
                    result.set(trigger.evaluate(conditionContext, null));
                } catch (Throwable t) {
                    thrown.set(t);
                } finally {
                    completed.countDown();
                }
            });
            runner.start();

            // Only proceed once the proxy confirms it actually swallowed an LPOP request:
            // evaluate() is now genuinely blocked on the sync command, so kill() is what has to
            // unblock it, not evaluate() finishing on its own.
            assertThat("evaluate() must reach the blocking lpop call before kill() is exercised",
                proxy.awaitLpopSwallowed(Duration.ofSeconds(10)), is(true));

            long killStart = System.currentTimeMillis();
            trigger.kill();
            long killElapsedMs = System.currentTimeMillis() - killStart;

            assertThat("Trigger.kill() must not block for the full maxDuration", killElapsedMs, lessThan(10000L));
            assertThat("evaluate() must return promptly after kill()", completed.await(10, TimeUnit.SECONDS), is(true));
            assertThat("A killed evaluate() must not be reported as a trigger error", thrown.get(), nullValue());
            assertThat("A killed evaluate() must not fire an execution", result.get().isPresent(), is(false));
        }
    }

    /**
     * A minimal transparent TCP proxy in front of a real Redis server: every RESP command is
     * forwarded as-is to the backend, except {@code LPOP}, which is parsed (to stay in sync with
     * the stream) but never forwarded and never answered, so the client's sync call hangs exactly
     * like it would against a genuinely stuck server or network.
     */
    private static final class BlockingLpopProxy implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final String backendHost;
        private final int backendPort;
        private final CountDownLatch lpopSwallowed = new CountDownLatch(1);

        private BlockingLpopProxy(ServerSocket serverSocket, String backendHost, int backendPort) {
            this.serverSocket = serverSocket;
            this.backendHost = backendHost;
            this.backendPort = backendPort;

            Thread acceptThread = new Thread(this::acceptLoop, "redis-blocking-lpop-proxy-accept");
            acceptThread.setDaemon(true);
            acceptThread.start();
        }

        static BlockingLpopProxy start(String backendHost, int backendPort) throws IOException {
            return new BlockingLpopProxy(new ServerSocket(0, 50, InetAddress.getLoopbackAddress()), backendHost, backendPort);
        }

        int getPort() {
            return serverSocket.getLocalPort();
        }

        boolean awaitLpopSwallowed(Duration timeout) throws InterruptedException {
            return lpopSwallowed.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        private void acceptLoop() {
            try {
                while (!serverSocket.isClosed()) {
                    Socket client = serverSocket.accept();
                    Thread clientThread = new Thread(() -> handleClient(client), "redis-blocking-lpop-proxy-client");
                    clientThread.setDaemon(true);
                    clientThread.start();
                }
            } catch (IOException ignored) {
                // server socket closed: proxy is shutting down
            }
        }

        private void handleClient(Socket client) {
            try (client; Socket backend = new Socket(backendHost, backendPort)) {
                Thread backendToClient = new Thread(() -> pipe(backend, client), "redis-blocking-lpop-proxy-b2c");
                backendToClient.setDaemon(true);
                backendToClient.start();

                InputStream in = client.getInputStream();
                OutputStream out = backend.getOutputStream();
                byte[] command;
                while ((command = readRespCommand(in)) != null) {
                    if (isLpop(command)) {
                        // Swallow: no forward, no response. The pending sync command on the
                        // client side now hangs until the connection is closed (by kill()).
                        lpopSwallowed.countDown();
                    } else {
                        out.write(command);
                        out.flush();
                    }
                }
            } catch (IOException ignored) {
                // client or backend closed the connection (e.g. kill() tearing down the socket)
            }
        }

        private static void pipe(Socket from, Socket to) {
            try (from; to) {
                from.getInputStream().transferTo(to.getOutputStream());
            } catch (IOException ignored) {
                // connection closed
            }
        }

        /**
         * Reads one full RESP array-of-bulk-strings command (the format every Redis client sends
         * requests in) and returns its raw bytes, or {@code null} on EOF before a new command starts.
         */
        private static byte[] readRespCommand(InputStream in) throws IOException {
            ByteArrayOutputStream raw = new ByteArrayOutputStream();
            Integer arity = readRespInt(in, raw, '*');
            if (arity == null) {
                return null;
            }
            for (int i = 0; i < arity; i++) {
                Integer length = readRespInt(in, raw, '$');
                if (length == null) {
                    return null;
                }
                readExactly(in, raw, length + 2); // payload + trailing CRLF
            }
            return raw.toByteArray();
        }

        /**
         * Reads a RESP header line ({@code "<prefix><digits>\r\n"}), writing every consumed byte
         * to {@code raw} so the caller can forward the command verbatim, and returns the parsed
         * integer, or {@code null} if the stream ended before a full line could be read.
         */
        private static Integer readRespInt(InputStream in, ByteArrayOutputStream raw, char prefix) throws IOException {
            int first = in.read();
            if (first == -1) {
                return null;
            }
            raw.write(first);
            if (first != prefix) {
                throw new IOException("Unexpected RESP prefix: " + (char) first);
            }

            StringBuilder digits = new StringBuilder();
            int b;
            while ((b = in.read()) != -1 && b != '\r') {
                raw.write(b);
                digits.append((char) b);
            }
            if (b == -1) {
                return null;
            }
            raw.write(b); // '\r'

            int lf = in.read();
            if (lf == -1) {
                return null;
            }
            raw.write(lf); // '\n'

            return Integer.parseInt(digits.toString());
        }

        private static void readExactly(InputStream in, ByteArrayOutputStream raw, int length) throws IOException {
            byte[] buffer = in.readNBytes(length);
            if (buffer.length != length) {
                throw new EOFException("Unexpected end of stream while reading RESP payload");
            }
            raw.write(buffer);
        }

        private static boolean isLpop(byte[] rawCommand) {
            // The command name is the first bulk string element; decoding it back out of the raw
            // bytes is simpler than threading it separately through readRespCommand's return value.
            String[] lines = new String(rawCommand, StandardCharsets.US_ASCII).split("\r\n");
            return lines.length > 2 && "LPOP".equalsIgnoreCase(lines[2]);
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
        }
    }
}
