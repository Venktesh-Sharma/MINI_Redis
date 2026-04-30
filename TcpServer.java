package com.miniredis.network;

import com.miniredis.commands.CommandProcessor;
import com.miniredis.core.DataStore;
import com.miniredis.persistence.AofPersistence;
import com.miniredis.pubsub.PubSubBroker;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * TCP Server — listens on a port and spawns a thread per client connection.
 *
 * Each client gets its own CommandProcessor instance (stateful per connection:
 * PubSub subscriptions, etc.). The DataStore and PubSubBroker are shared
 * across all clients (singletons injected at construction).
 *
 * Thread model: cached thread pool (scales with connections, not fixed).
 */
public class TcpServer {

    private static final Logger log = Logger.getLogger(TcpServer.class.getName());

    private final int port;
    private final DataStore dataStore;
    private final AofPersistence aof;
    private final PubSubBroker pubSub;

    private ServerSocket serverSocket;
    private volatile boolean running = false;

    // Thread pool for client handlers
    private final ExecutorService clientPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
    });

    private final AtomicInteger connectedClients = new AtomicInteger(0);

    public TcpServer(int port, DataStore dataStore, AofPersistence aof, PubSubBroker pubSub) {
        this.port      = port;
        this.dataStore = dataStore;
        this.aof       = aof;
        this.pubSub    = pubSub;
    }

    /**
     * Starts the server and blocks the calling thread in the accept loop.
     */
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        running = true;

        log.info("╔══════════════════════════════════════╗");
        log.info("║     Mini-Redis Server Started         ║");
        log.info("║     Listening on port: " + port + "          ║");
        log.info("╚══════════════════════════════════════╝");

        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                int count = connectedClients.incrementAndGet();
                log.info("[Server] New connection from " + clientSocket.getRemoteSocketAddress()
                    + " (active clients: " + count + ")");
                clientPool.submit(new ClientHandler(clientSocket));
            } catch (SocketException e) {
                if (running) log.warning("[Server] Accept error: " + e.getMessage());
            }
        }
    }

    public void shutdown() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) serverSocket.close();
        } catch (IOException ignored) {}
        clientPool.shutdown();
        try {
            if (!clientPool.awaitTermination(5, TimeUnit.SECONDS)) {
                clientPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("[Server] Shutdown complete.");
    }

    // ─── Client Handler ──────────────────────────────────────────────────────────

    /**
     * Handles one client connection: reads commands line-by-line, writes responses.
     */
    private class ClientHandler implements Runnable {

        private final Socket socket;
        private final CommandProcessor processor;

        ClientHandler(Socket socket) {
            this.socket    = socket;
            this.processor = new CommandProcessor(dataStore, aof, pubSub);
        }

        @Override
        public void run() {
            try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
                 PrintWriter writer = new PrintWriter(
                        new OutputStreamWriter(socket.getOutputStream()), true)) {

                // Greeting
                writer.println("+Mini-Redis 1.0.0 — Type HELP for commands.");

                String line;
                while ((line = reader.readLine()) != null) {
                    String response = processor.process(line.trim());
                    writer.println(response);

                    // Flush any PubSub messages received since last command
                    String pending = processor.drainPendingMessages();
                    if (pending != null) writer.println(pending);

                    if (response.equals("+BYE")) break;
                }

            } catch (SocketException e) {
                log.fine("[ClientHandler] Client disconnected: " + socket.getRemoteSocketAddress());
            } catch (IOException e) {
                log.warning("[ClientHandler] IO error: " + e.getMessage());
            } finally {
                connectedClients.decrementAndGet();
                try { socket.close(); } catch (IOException ignored) {}
                log.info("[Server] Client disconnected. Active clients: " + connectedClients.get());
            }
        }
    }
}
