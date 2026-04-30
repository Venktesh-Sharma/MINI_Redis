package com.miniredis;

import com.miniredis.core.DataStore;
import org.junit.jupiter.api.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DataStore covering:
 *   - Basic CRUD
 *   - TTL expiry
 *   - INCR/DECR atomicity under concurrency
 *   - LRU eviction
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DataStoreTest {

    private DataStore store;

    @BeforeEach
    void setUp() {
        store = new DataStore(100);
    }

    @AfterEach
    void tearDown() {
        store.shutdown();
    }

    // ─── Basic CRUD ──────────────────────────────────────────────────────────────

    @Test @Order(1)
    void testSetAndGet() {
        store.set("name", "Rajrishi");
        assertEquals("Rajrishi", store.get("name"));
    }

    @Test @Order(2)
    void testGetMissingKey() {
        assertNull(store.get("nonexistent"));
    }

    @Test @Order(3)
    void testDelete() {
        store.set("key", "val");
        assertTrue(store.delete("key"));
        assertNull(store.get("key"));
        assertFalse(store.delete("key")); // second delete returns false
    }

    @Test @Order(4)
    void testExists() {
        store.set("x", "1");
        assertTrue(store.exists("x"));
        store.delete("x");
        assertFalse(store.exists("x"));
    }

    @Test @Order(5)
    void testOverwrite() {
        store.set("k", "v1");
        store.set("k", "v2");
        assertEquals("v2", store.get("k"));
    }

    // ─── TTL ────────────────────────────────────────────────────────────────────

    @Test @Order(6)
    void testTtlExpiry() throws InterruptedException {
        store.set("temp", "data");
        store.expire("temp", 1); // 1 second TTL
        assertEquals("data", store.get("temp")); // still alive
        Thread.sleep(1200);                       // wait for expiry
        assertNull(store.get("temp"));            // should be expired
    }

    @Test @Order(7)
    void testTtlReturnsNegativeForNoTtl() {
        store.set("permanent", "value");
        assertEquals(-1L, store.ttl("permanent"));
    }

    @Test @Order(8)
    void testTtlReturnsNegativeTwoForMissingKey() {
        assertEquals(-2L, store.ttl("ghost"));
    }

    @Test @Order(9)
    void testExpireReturnsFalseForMissingKey() {
        assertFalse(store.expire("ghost", 10));
    }

    @Test @Order(10)
    void testSetClearsTtl() throws InterruptedException {
        store.set("key", "v1");
        store.expire("key", 1);
        store.set("key", "v2"); // should clear TTL
        Thread.sleep(1200);
        assertEquals("v2", store.get("key")); // should still be alive
    }

    // ─── INCR / DECR ────────────────────────────────────────────────────────────

    @Test @Order(11)
    void testIncrOnNewKey() {
        assertEquals(1L, store.incr("counter"));
        assertEquals(2L, store.incr("counter"));
    }

    @Test @Order(12)
    void testDecrBelowZero() {
        assertEquals(-1L, store.decr("counter"));
    }

    @Test @Order(13)
    void testIncrByDelta() {
        store.set("n", "10");
        assertEquals(15L, store.incrBy("n", 5));
        assertEquals(12L, store.incrBy("n", -3));
    }

    @Test @Order(14)
    void testIncrOnNonNumericThrows() {
        store.set("k", "hello");
        assertThrows(NumberFormatException.class, () -> store.incr("k"));
    }

    // ─── Concurrency ────────────────────────────────────────────────────────────

    @Test @Order(15)
    void testConcurrentIncr() throws InterruptedException {
        int threads  = 50;
        int perThread = 100;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                for (int j = 0; j < perThread; j++) store.incr("shared");
                latch.countDown();
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        long expected = (long) threads * perThread;
        assertEquals(expected, Long.parseLong(store.get("shared")));
    }

    @Test @Order(16)
    void testConcurrentSetGet() throws InterruptedException {
        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            final int id = i;
            pool.submit(() -> {
                store.set("key" + id, "val" + id);
                assertEquals("val" + id, store.get("key" + id));
                latch.countDown();
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        pool.shutdown();
    }

    // ─── LRU Eviction ───────────────────────────────────────────────────────────

    @Test @Order(17)
    void testLruEvictionOnCapacityExceeded() {
        DataStore smallStore = new DataStore(3); // capacity = 3
        smallStore.set("a", "1");
        smallStore.set("b", "2");
        smallStore.set("c", "3");

        // Access "a" to make it recently used
        smallStore.get("a");

        // Add 4th key → "b" should be evicted (LRU: b was accessed least recently)
        smallStore.set("d", "4");

        // "a" was recently used → should survive
        assertNotNull(smallStore.get("a"));
        // "d" is new → should survive
        assertNotNull(smallStore.get("d"));
        // "b" was LRU → should be evicted
        assertNull(smallStore.get("b"));

        smallStore.shutdown();
    }

    // ─── Keys / Size ────────────────────────────────────────────────────────────

    @Test @Order(18)
    void testKeysAndSize() {
        store.set("k1", "v1");
        store.set("k2", "v2");
        assertEquals(2, store.size());
        assertTrue(store.keys().contains("k1"));
        assertTrue(store.keys().contains("k2"));
    }

    @Test @Order(19)
    void testFlushAll() {
        store.set("k1", "v1");
        store.set("k2", "v2");
        store.flushAll();
        assertEquals(0, store.size());
    }
}
