package com.miniredis.core;

import com.miniredis.eviction.LRUCache;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Core in-memory data store with TTL support, LRU eviction, and thread safety.
 * All public methods are safe to call from multiple threads concurrently.
 */
public class DataStore {

    // Main storage: key -> ValueEntry (wraps value + metadata)
    private final LRUCache<String, ValueEntry> store;

    // TTL registry: key -> expiry timestamp in milliseconds
    private final Map<String, Long> ttlMap = new ConcurrentHashMap<>();

    // Read-write lock for atomic compound operations (e.g., INCR)
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // Background thread that purges expired keys every second
    private final ScheduledExecutorService ttlCleaner = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ttl-cleaner");
        t.setDaemon(true);
        return t;
    });

    public DataStore(int maxCapacity) {
        this.store = new LRUCache<>(maxCapacity);
        // Schedule TTL cleanup every 500ms
        ttlCleaner.scheduleAtFixedRate(this::purgeExpiredKeys, 500, 500, TimeUnit.MILLISECONDS);
    }

    // ─── SET ────────────────────────────────────────────────────────────────────

    /**
     * Store a key-value pair. Overwrites existing value and clears any TTL.
     */
    public void set(String key, String value) {
        rwLock.writeLock().lock();
        try {
            store.put(key, new ValueEntry(value));
            ttlMap.remove(key); // reset TTL on re-set
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    // ─── GET ────────────────────────────────────────────────────────────────────

    /**
     * Retrieve value. Returns null if key doesn't exist or has expired.
     */
    public String get(String key) {
        rwLock.readLock().lock();
        try {
            if (isExpired(key)) {
                return null;
            }
            ValueEntry entry = store.get(key);
            return entry != null ? entry.getValue() : null;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    // ─── DELETE ─────────────────────────────────────────────────────────────────

    /**
     * Delete a key. Returns true if key existed and was deleted.
     */
    public boolean delete(String key) {
        rwLock.writeLock().lock();
        try {
            ttlMap.remove(key);
            return store.remove(key) != null;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    // ─── EXISTS ─────────────────────────────────────────────────────────────────

    /**
     * Check if a key exists and has not expired.
     */
    public boolean exists(String key) {
        rwLock.readLock().lock();
        try {
            return !isExpired(key) && store.containsKey(key);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    // ─── EXPIRE ─────────────────────────────────────────────────────────────────

    /**
     * Set a TTL (seconds) on an existing key. Returns false if key doesn't exist.
     */
    public boolean expire(String key, long seconds) {
        rwLock.writeLock().lock();
        try {
            if (!store.containsKey(key) || isExpired(key)) {
                return false;
            }
            long expiryMs = System.currentTimeMillis() + (seconds * 1000L);
            ttlMap.put(key, expiryMs);
            return true;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Returns remaining TTL in seconds. -1 = no TTL, -2 = key doesn't exist.
     */
    public long ttl(String key) {
        rwLock.readLock().lock();
        try {
            if (!store.containsKey(key) || isExpired(key)) return -2L;
            Long expiry = ttlMap.get(key);
            if (expiry == null) return -1L;
            long remaining = expiry - System.currentTimeMillis();
            return remaining > 0 ? remaining / 1000L : -2L;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    // ─── INCR / DECR ────────────────────────────────────────────────────────────

    /**
     * Atomically increment the integer value of a key by delta.
     * Creates key with value 0 if it doesn't exist, then increments.
     */
    public long incrBy(String key, long delta) {
        rwLock.writeLock().lock();
        try {
            String current = get(key);
            long val = (current == null) ? 0L : Long.parseLong(current);
            long newVal = val + delta;
            store.put(key, new ValueEntry(String.valueOf(newVal)));
            return newVal;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public long incr(String key) { return incrBy(key, 1); }
    public long decr(String key) { return incrBy(key, -1); }

    // ─── KEYS / FLUSH ───────────────────────────────────────────────────────────

    public Set<String> keys() {
        return store.keySet();
    }

    public int size() {
        return store.size();
    }

    public void flushAll() {
        rwLock.writeLock().lock();
        try {
            store.clear();
            ttlMap.clear();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    // ─── Internal helpers ────────────────────────────────────────────────────────

    private boolean isExpired(String key) {
        Long expiry = ttlMap.get(key);
        if (expiry == null) return false;
        if (System.currentTimeMillis() > expiry) {
            // Lazy delete: remove here (caller holds at least readLock — upgrade needed)
            // We can only safely remove in write context; mark for cleanup
            return true;
        }
        return false;
    }

    /** Background TTL purge — runs every 500ms */
    private void purgeExpiredKeys() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry : ttlMap.entrySet()) {
            if (now > entry.getValue()) {
                rwLock.writeLock().lock();
                try {
                    store.remove(entry.getKey());
                    ttlMap.remove(entry.getKey());
                } finally {
                    rwLock.writeLock().unlock();
                }
            }
        }
    }

    /** Package-visible: raw snapshot of store for persistence */
    Map<String, ValueEntry> getRawStore() {
        return store.snapshotMap();
    }

    Map<String, Long> getRawTtlMap() {
        return java.util.Collections.unmodifiableMap(ttlMap);
    }

    /** Restore a key-value pair during recovery (bypasses LRU touch) */
    public void restoreEntry(String key, String value, Long expiryMs) {
        rwLock.writeLock().lock();
        try {
            // Skip if TTL already passed
            if (expiryMs != null && System.currentTimeMillis() > expiryMs) return;
            store.put(key, new ValueEntry(value));
            if (expiryMs != null) ttlMap.put(key, expiryMs);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void shutdown() {
        ttlCleaner.shutdownNow();
    }
}
