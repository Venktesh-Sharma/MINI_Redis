package com.miniredis.eviction;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Thread-safe LRU Cache backed by a synchronized LinkedHashMap in access-order mode.
 * When capacity is exceeded, the least-recently-used entry is evicted automatically.
 *
 * Time complexity:
 *   get()  → O(1)
 *   put()  → O(1) amortized
 *   remove → O(1)
 */
public class LRUCache<K, V> {

    private final int capacity;

    // LinkedHashMap in access-order mode (true = access order, false = insertion order)
    private final Map<K, V> cache;

    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.cache = Collections.synchronizedMap(
            new LinkedHashMap<K, V>(capacity, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                    if (size() > LRUCache.this.capacity) {
                        System.out.printf("[LRU] Evicting key: %s%n", eldest.getKey());
                        return true;
                    }
                    return false;
                }
            }
        );
    }

    public V get(K key) {
        return cache.get(key);
    }

    public void put(K key, V value) {
        cache.put(key, value);
    }

    public V remove(K key) {
        return cache.remove(key);
    }

    public boolean containsKey(K key) {
        return cache.containsKey(key);
    }

    public Set<K> keySet() {
        return cache.keySet();
    }

    public int size() {
        return cache.size();
    }

    public void clear() {
        cache.clear();
    }

    /** Returns a stable snapshot (copy) for persistence / iteration */
    public Map<K, V> snapshotMap() {
        synchronized (cache) {
            return new java.util.HashMap<>(cache);
        }
    }
}
