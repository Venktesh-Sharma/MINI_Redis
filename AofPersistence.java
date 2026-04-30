package com.miniredis.persistence;

import com.miniredis.core.DataStore;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Append-Only File (AOF) persistence.
 *
 * Every write command (SET, DELETE, EXPIRE, INCR, DECR) is appended to a log file.
 * On startup, the AOF is replayed to restore state.
 *
 * Format (one command per line, pipe-delimited):
 *   SET|key|value
 *   DEL|key
 *   EXPIRE|key|epochMs
 *   INCR|key|delta
 */
public class AofPersistence {

    private static final Logger log = Logger.getLogger(AofPersistence.class.getName());
    private static final String AOF_FILE = "miniredis.aof";
    private static final String AOF_TEMP  = "miniredis.aof.tmp";

    private final DataStore dataStore;
    private PrintWriter writer;
    private final Object writeLock = new Object();

    // Background rewrite compaction: every 5 minutes
    private final ScheduledExecutorService compactor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "aof-compactor");
        t.setDaemon(true);
        return t;
    });

    public AofPersistence(DataStore dataStore) throws IOException {
        this.dataStore = dataStore;
        openWriter();
        compactor.scheduleAtFixedRate(this::compactAof, 5, 5, TimeUnit.MINUTES);
    }

    private void openWriter() throws IOException {
        FileWriter fw = new FileWriter(AOF_FILE, StandardCharsets.UTF_8, true); // append=true
        writer = new PrintWriter(new BufferedWriter(fw));
    }

    // ─── Log commands ────────────────────────────────────────────────────────────

    public void logSet(String key, String value) {
        append("SET|" + escape(key) + "|" + escape(value));
    }

    public void logDelete(String key) {
        append("DEL|" + escape(key));
    }

    public void logExpire(String key, long expiryEpochMs) {
        append("EXPIRE|" + escape(key) + "|" + expiryEpochMs);
    }

    public void logIncrBy(String key, long delta) {
        append("INCRBY|" + escape(key) + "|" + delta);
    }

    public void logFlush() {
        append("FLUSHALL");
    }

    private void append(String line) {
        synchronized (writeLock) {
            writer.println(line);
            writer.flush();
        }
    }

    // ─── Replay on startup ───────────────────────────────────────────────────────

    /**
     * Replay the AOF file to restore DataStore state.
     * Called once at server startup before accepting connections.
     */
    public void replay() {
        Path path = Paths.get(AOF_FILE);
        if (!Files.exists(path)) {
            log.info("[AOF] No existing AOF file found. Starting fresh.");
            return;
        }

        log.info("[AOF] Replaying " + AOF_FILE + " ...");
        int count = 0;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(AOF_FILE), StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                processAofLine(line);
                count++;
            }

        } catch (IOException e) {
            log.warning("[AOF] Error during replay: " + e.getMessage());
        }

        log.info("[AOF] Replayed " + count + " commands.");
    }

    private void processAofLine(String line) {
        String[] parts = line.split("\\|", -1);
        if (parts.length == 0) return;

        String cmd = parts[0];
        try {
            switch (cmd) {
                case "SET" -> {
                    if (parts.length >= 3)
                        dataStore.restoreEntry(unescape(parts[1]), unescape(parts[2]), null);
                }
                case "DEL" -> {
                    if (parts.length >= 2) dataStore.delete(unescape(parts[1]));
                }
                case "EXPIRE" -> {
                    if (parts.length >= 3) {
                        long expiryMs = Long.parseLong(parts[2]);
                        dataStore.restoreEntry(
                            unescape(parts[1]),
                            dataStore.get(unescape(parts[1])) != null ? dataStore.get(unescape(parts[1])) : "",
                            expiryMs
                        );
                    }
                }
                case "INCRBY" -> {
                    if (parts.length >= 3) dataStore.incrBy(unescape(parts[1]), Long.parseLong(parts[2]));
                }
                case "FLUSHALL" -> dataStore.flushAll();
                default -> log.warning("[AOF] Unknown command in AOF: " + cmd);
            }
        } catch (Exception e) {
            log.warning("[AOF] Error processing line '" + line + "': " + e.getMessage());
        }
    }

    // ─── AOF Compaction ─────────────────────────────────────────────────────────

    /**
     * Compacts the AOF by rewriting it from current DataStore state.
     * This prevents unbounded AOF growth. Atomic swap via temp file.
     */
    public void compactAof() {
        log.info("[AOF] Starting compaction...");
        try {
            // Write current state to temp file
            try (PrintWriter tempWriter = new PrintWriter(new BufferedWriter(
                    new FileWriter(AOF_TEMP, StandardCharsets.UTF_8, false)))) {

                // Snapshot current keys (SET commands) — iterate over a stable copy
                for (String key : new java.util.ArrayList<>(dataStore.keys())) {
                    String value = dataStore.get(key);
                    if (value == null) continue; // expired during iteration
                    tempWriter.println("SET|" + escape(key) + "|" + escape(value));

                    // Include TTL if exists
                    long ttlSec = dataStore.ttl(key);
                    if (ttlSec > 0) {
                        long expiryMs = System.currentTimeMillis() + (ttlSec * 1000L);
                        tempWriter.println("EXPIRE|" + escape(key) + "|" + expiryMs);
                    }
                }
            }

            // Atomic rename: temp → main AOF
            synchronized (writeLock) {
                writer.close();
                Files.move(Paths.get(AOF_TEMP), Paths.get(AOF_FILE),
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                openWriter();
            }
            log.info("[AOF] Compaction complete.");

        } catch (IOException e) {
            log.warning("[AOF] Compaction failed: " + e.getMessage());
        }
    }

    public void shutdown() {
        compactor.shutdownNow();
        synchronized (writeLock) {
            if (writer != null) writer.close();
        }
    }

    // Simple escape: replace | with \pipe to avoid splitting issues
    private String escape(String s)   { return s == null ? "" : s.replace("\\", "\\\\").replace("|", "\\pipe"); }
    private String unescape(String s) { return s == null ? "" : s.replace("\\pipe", "|").replace("\\\\", "\\"); }
}
