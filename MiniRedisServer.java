package com.miniredis;

import com.miniredis.core.DataStore;
import com.miniredis.network.TcpServer;
import com.miniredis.persistence.AofPersistence;
import com.miniredis.pubsub.PubSubBroker;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Mini-Redis Server Bootstrap.
 *
 * Wires together all subsystems:
 *   DataStore ← AOF Persistence ← PubSub ← TCP Server
 *
 * Default port: 6399 (avoids collision with real Redis on 6379).
 * Override via: java -jar miniredis.jar [port] [maxKeys]
 */
public class MiniRedisServer {

    private static final Logger log = Logger.getLogger(MiniRedisServer.class.getName());

    private static final int DEFAULT_PORT     = 6399;
    private static final int DEFAULT_MAX_KEYS = 10_000;

    public static void main(String[] args) throws IOException {
        int port    = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        int maxKeys = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_MAX_KEYS;

        // 1. Core data store with LRU eviction
        DataStore dataStore = new DataStore(maxKeys);

        // 2. Persistence layer (AOF)
        AofPersistence aof = new AofPersistence(dataStore);
        aof.replay(); // Restore from disk before accepting connections

        // 3. Pub/Sub broker
        PubSubBroker pubSub = new PubSubBroker();

        // 4. TCP server
        TcpServer server = new TcpServer(port, dataStore, aof, pubSub);

        // Graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("[Bootstrap] Shutdown signal received...");
            server.shutdown();
            aof.compactAof(); // Final AOF compaction on shutdown
            aof.shutdown();
            dataStore.shutdown();
            log.info("[Bootstrap] Goodbye!");
        }, "shutdown-hook"));

        // Start blocking accept loop
        server.start();
    }
}
