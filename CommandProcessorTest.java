package com.miniredis;

import com.miniredis.commands.CommandProcessor;
import com.miniredis.core.DataStore;
import com.miniredis.persistence.AofPersistence;
import com.miniredis.pubsub.PubSubBroker;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CommandProcessorTest {

    private static CommandProcessor processor;
    private static DataStore store;

    @BeforeAll
    static void setup() throws IOException {
        store = new DataStore(1000);
        AofPersistence aof = new AofPersistence(store);
        PubSubBroker pubSub = new PubSubBroker();
        processor = new CommandProcessor(store, aof, pubSub);
        // Cleanup test AOF
        Files.deleteIfExists(Paths.get("miniredis.aof"));
    }

    @AfterAll
    static void teardown() {
        store.shutdown();
        try { Files.deleteIfExists(Paths.get("miniredis.aof")); } catch (Exception ignored) {}
    }

    @Test @Order(1)  void ping()        { assertEquals("+PONG", processor.process("PING")); }
    @Test @Order(2)  void setGet()      { processor.process("SET city Bangalore"); assertEquals("$Bangalore", processor.process("GET city")); }
    @Test @Order(3)  void getMissing()  { assertEquals("$nil", processor.process("GET ghost")); }
    @Test @Order(4)  void del()         { processor.process("SET tmp x"); assertEquals(":1", processor.process("DEL tmp")); }
    @Test @Order(5)  void exists()      { processor.process("SET ex 1"); assertEquals(":1", processor.process("EXISTS ex")); }
    @Test @Order(6)  void notExists()   { assertEquals(":0", processor.process("EXISTS nope")); }
    @Test @Order(7)  void incr()        { processor.process("SET c 10"); assertEquals(":11", processor.process("INCR c")); }
    @Test @Order(8)  void decr()        { processor.process("SET c 5");  assertEquals(":4",  processor.process("DECR c")); }
    @Test @Order(9)  void incrBy()      { processor.process("SET n 10"); assertEquals(":15", processor.process("INCRBY n 5")); }
    @Test @Order(10) void decrBy()      { processor.process("SET n 10"); assertEquals(":7",  processor.process("DECRBY n 3")); }
    @Test @Order(11) void setWithEx()   { processor.process("SET ttlkey val EX 60"); assertTrue(Long.parseLong(processor.process("TTL ttlkey").substring(1)) > 0); }
    @Test @Order(12) void dbsize()      { assertTrue(processor.process("DBSIZE").contains("DBSIZE")); }
    @Test @Order(13) void flushAll()    { processor.process("SET a 1"); processor.process("FLUSHALL"); assertEquals("$nil", processor.process("GET a")); }
    @Test @Order(14) void unknown()     { assertTrue(processor.process("FOOBAR").startsWith("-ERR")); }
    @Test @Order(15) void quit()        { assertEquals("+BYE", processor.process("QUIT")); }

    @Test @Order(16)
    void tokenizeQuoted() {
        String[] tokens = CommandProcessor.tokenize("PUBLISH news \"hello world\"");
        assertEquals(3, tokens.length);
        assertEquals("hello world", tokens[2]);
    }

    @Test @Order(17)
    void pubSubPublish() {
        String sub  = processor.process("SUBSCRIBE sports");
        String pub  = processor.process("PUBLISH sports goalscored");
        assertTrue(sub.startsWith("+SUBSCRIBED"));
        assertTrue(pub.contains("1")); // 1 subscriber received it
    }
}
