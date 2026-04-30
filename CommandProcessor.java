package com.miniredis.commands;

import com.miniredis.core.DataStore;
import com.miniredis.persistence.AofPersistence;
import com.miniredis.pubsub.PubSubBroker;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;

/**
 * CommandProcessor parses raw command strings and dispatches to DataStore / PubSub.
 *
 * Protocol: space-delimited tokens.
 *   SET name Venktesh
 *   GET name
 *   EXPIRE name 60
 *   SUBSCRIBE news
 *   PUBLISH news "Hello world"
 *
 * Returns a string response (similar to Redis inline response format).
 */
public class CommandProcessor {

    private final DataStore store;
    private final AofPersistence aof;
    private final PubSubBroker pubSub;

    // Unique ID for this connection's pub/sub subscriptions
    private final String subscriberId = UUID.randomUUID().toString();

    // Reference to subscription listener so it can be removed on UNSUBSCRIBE
    private PubSubBroker.Listener activeListener;
    private final StringBuilder pendingMessages = new StringBuilder();

    public CommandProcessor(DataStore store, AofPersistence aof, PubSubBroker pubSub) {
        this.store = store;
        this.aof = aof;
        this.pubSub = pubSub;
    }

    /**
     * Process a raw command string and return the response.
     */
    public String process(String rawCommand) {
        if (rawCommand == null || rawCommand.isBlank()) return "-ERR empty command";

        // Tokenize: handle quoted strings
        String[] tokens = tokenize(rawCommand.trim());
        if (tokens.length == 0) return "-ERR empty command";

        String cmd = tokens[0].toUpperCase();

        try {
            return switch (cmd) {
                case "SET"        -> handleSet(tokens);
                case "GET"        -> handleGet(tokens);
                case "DEL",
                     "DELETE"     -> handleDelete(tokens);
                case "EXISTS"     -> handleExists(tokens);
                case "EXPIRE"     -> handleExpire(tokens);
                case "TTL"        -> handleTtl(tokens);
                case "INCR"       -> handleIncr(tokens);
                case "DECR"       -> handleDecr(tokens);
                case "INCRBY"     -> handleIncrBy(tokens);
                case "DECRBY"     -> handleDecrBy(tokens);
                case "KEYS"       -> handleKeys();
                case "FLUSHALL"   -> handleFlushAll();
                case "DBSIZE"     -> "+DBSIZE " + store.size();
                case "SUBSCRIBE"  -> handleSubscribe(tokens);
                case "UNSUBSCRIBE"-> handleUnsubscribe(tokens);
                case "PUBLISH"    -> handlePublish(tokens);
                case "PING"       -> "+PONG";
                case "QUIT",
                     "EXIT"       -> "+BYE";
                case "HELP"       -> buildHelp();
                default           -> "-ERR unknown command '" + cmd + "'";
            };
        } catch (NumberFormatException e) {
            return "-ERR value is not an integer or out of range";
        } catch (Exception e) {
            return "-ERR " + e.getMessage();
        }
    }

    // ─── Command Handlers ────────────────────────────────────────────────────────

    private String handleSet(String[] t) {
        if (t.length < 3) return "-ERR syntax: SET key value [EX seconds]";
        String key = t[1], value = t[2];
        store.set(key, value);
        aof.logSet(key, value);
        // Support inline EX: SET key value EX 60
        if (t.length >= 5 && t[3].equalsIgnoreCase("EX")) {
            long seconds = Long.parseLong(t[4]);
            store.expire(key, seconds);
            aof.logExpire(key, System.currentTimeMillis() + seconds * 1000L);
        }
        return "+OK";
    }

    private String handleGet(String[] t) {
        if (t.length < 2) return "-ERR syntax: GET key";
        String val = store.get(t[1]);
        return val != null ? "$" + val : "$nil";
    }

    private String handleDelete(String[] t) {
        if (t.length < 2) return "-ERR syntax: DEL key [key ...]";
        int deleted = 0;
        for (int i = 1; i < t.length; i++) {
            if (store.delete(t[i])) {
                aof.logDelete(t[i]);
                deleted++;
            }
        }
        return ":" + deleted;
    }

    private String handleExists(String[] t) {
        if (t.length < 2) return "-ERR syntax: EXISTS key";
        return store.exists(t[1]) ? ":1" : ":0";
    }

    private String handleExpire(String[] t) {
        if (t.length < 3) return "-ERR syntax: EXPIRE key seconds";
        boolean ok = store.expire(t[1], Long.parseLong(t[2]));
        if (ok) aof.logExpire(t[1], System.currentTimeMillis() + Long.parseLong(t[2]) * 1000L);
        return ok ? ":1" : ":0";
    }

    private String handleTtl(String[] t) {
        if (t.length < 2) return "-ERR syntax: TTL key";
        return ":" + store.ttl(t[1]);
    }

    private String handleIncr(String[] t) {
        if (t.length < 2) return "-ERR syntax: INCR key";
        long val = store.incr(t[1]);
        aof.logIncrBy(t[1], 1);
        return ":" + val;
    }

    private String handleDecr(String[] t) {
        if (t.length < 2) return "-ERR syntax: DECR key";
        long val = store.decr(t[1]);
        aof.logIncrBy(t[1], -1);
        return ":" + val;
    }

    private String handleIncrBy(String[] t) {
        if (t.length < 3) return "-ERR syntax: INCRBY key delta";
        long delta = Long.parseLong(t[2]);
        long val = store.incrBy(t[1], delta);
        aof.logIncrBy(t[1], delta);
        return ":" + val;
    }

    private String handleDecrBy(String[] t) {
        if (t.length < 3) return "-ERR syntax: DECRBY key delta";
        long delta = Long.parseLong(t[2]);
        long val = store.incrBy(t[1], -delta);
        aof.logIncrBy(t[1], -delta);
        return ":" + val;
    }

    private String handleKeys() {
        Set<String> keys = store.keys();
        if (keys.isEmpty()) return "*0";
        StringBuilder sb = new StringBuilder("*" + keys.size() + "\n");
        for (String k : keys) sb.append("  ").append(k).append("\n");
        return sb.toString().trim();
    }

    private String handleFlushAll() {
        store.flushAll();
        aof.logFlush();
        return "+OK";
    }

    private String handleSubscribe(String[] t) {
        if (t.length < 2) return "-ERR syntax: SUBSCRIBE channel";
        String channel = t[1];
        activeListener = (ch, msg) -> pendingMessages.append("*MESSAGE ").append(ch).append(": ").append(msg).append("\n");
        int count = pubSub.subscribe(channel, subscriberId, activeListener);
        return "+SUBSCRIBED " + channel + " (" + count + " subscribers)";
    }

    private String handleUnsubscribe(String[] t) {
        if (t.length < 2) return "-ERR syntax: UNSUBSCRIBE channel";
        String channel = t[1];
        if (activeListener == null) return "-ERR not subscribed to any channel";
        int remaining = pubSub.unsubscribe(channel, subscriberId, activeListener);
        activeListener = null;
        return "+UNSUBSCRIBED " + channel + " (" + remaining + " remaining)";
    }

    private String handlePublish(String[] t) {
        if (t.length < 3) return "-ERR syntax: PUBLISH channel message";
        String channel = t[1];
        // Join remaining tokens as message
        String message = String.join(" ", Arrays.copyOfRange(t, 2, t.length));
        int receivers = pubSub.publish(channel, message);
        return ":PUBLISHED to " + receivers + " subscriber(s)";
    }

    private String buildHelp() {
        return """
            +Mini-Redis Commands:
              SET key value [EX seconds]  - Store a value
              GET key                     - Retrieve a value
              DEL key [key ...]           - Delete one or more keys
              EXISTS key                  - Check if key exists (1/0)
              EXPIRE key seconds          - Set TTL on a key
              TTL key                     - Get remaining TTL (-1=no ttl, -2=missing)
              INCR key                    - Increment integer value
              DECR key                    - Decrement integer value
              INCRBY key delta            - Increment by delta
              DECRBY key delta            - Decrement by delta
              KEYS                        - List all keys
              FLUSHALL                    - Delete all keys
              DBSIZE                      - Number of keys
              SUBSCRIBE channel           - Subscribe to pub/sub channel
              UNSUBSCRIBE channel         - Unsubscribe from channel
              PUBLISH channel message     - Publish message to channel
              PING                        - Health check
              QUIT / EXIT                 - Disconnect
              HELP                        - This help text""";
    }

    // ─── Tokenizer ───────────────────────────────────────────────────────────────

    /**
     * Splits command into tokens, respecting double-quoted strings.
     * Example: PUBLISH news "hello world" → ["PUBLISH", "news", "hello world"]
     */
    static String[] tokenize(String input) {
        java.util.List<String> tokens = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '"') {
                inQuote = !inQuote;
            } else if (c == ' ' && !inQuote) {
                if (!current.isEmpty()) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }
            } else {
                current.append(c);
            }
        }
        if (!current.isEmpty()) tokens.add(current.toString());
        return tokens.toArray(new String[0]);
    }

    /** Drain any pending pub/sub messages (for async display on client). */
    public String drainPendingMessages() {
        if (pendingMessages.isEmpty()) return null;
        String msgs = pendingMessages.toString();
        pendingMessages.setLength(0);
        return msgs;
    }
}
