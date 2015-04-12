package org.apache.kafka.clients.streaming.kv;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.streaming.ProcessorContext;
import org.apache.kafka.clients.streaming.StreamingConfig;
import org.apache.kafka.common.Entry;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

public abstract class AbstractKeyValueStoreTest {

    private final int iterations;

    public AbstractKeyValueStoreTest(int iterations) {
        this.iterations = iterations;
    }

    public ProcessorContext processorContext(Properties properties) {
        return new ProcessorContext(0,
                                    new StreamingConfig(properties),
                                    TestUtils.tempDir(),
                                    new HashSet<TopicPartition>(),
                                    new Metrics());
    }

    public abstract KeyValueStore<String, String> store();

    private void putRange(int from, int to) {
        for (int i = from; i < to; i++)
            store().put(Integer.toString(i), Integer.toString(i));
    }

    private void validateRange(int from, int to) {
        for (int i = 0; i < iterations; i++)
            assertEquals(Integer.toString(i), store().get(Integer.toString(i)));
    }

    private void validateIterator(KeyValueIterator<String, String> iter, int from, int to) {
        List<String> expected = new ArrayList<String>(to - from);
        for (int i = from; i < to; i++)
            expected.add(Integer.toString(i));
        Collections.sort(expected);
        for(String value: expected) {
            assertTrue(iter.hasNext());
            Entry<String, String> entry = iter.next();
            assertEquals(value, entry.key());
            assertEquals(value, entry.value());
        }
        assertFalse(iter.hasNext());
        iter.close();
    }

    @Test
    public void getNonExistantIsNull() {
        assertNull(store().get("hello"));
    }

    @Test
    public void putAndGet() {
        store().put("k", "v");
        assertEquals("v", store().get("k"));
    }

    @Test
    public void putGetStessTest() {
        putRange(0, iterations);
        validateRange(0, iterations);
        putRange(0, iterations);
        validateRange(0, iterations);
    }

    @Test
    public void doublePutAndGet() {
        String k = "k";
        store().put(k, "v1");
        store().put(k, "v2");
        store().put(k, "v3");
        assertEquals("v3", store().get(k));
    }

    // TODO: add tests for null key/value pairs

    @Test
    public void testPutAll() {
        List<Entry<String, String>> entries = new ArrayList<Entry<String, String>>();
        for (int i = 0; i < iterations; i++)
            entries.add(new Entry<String, String>(Integer.toString(i), Integer.toString(i)));
        store().putAll(entries);
        validateRange(0, iterations);
    }

    @Test
    public void testIterateAll() {
        putRange(0, iterations);
        validateIterator(store().all(), 0, iterations);
        validateIterator(store().all(), 0, iterations);
    }

    @Test
    public void testRange() {
        int from = 3;
        int to = 7;
        putRange(0, 9);
        validateIterator(store().range(Integer.toString(from), Integer.toString(to)), from, to);
    }

    @Test
    public void testDelete() {
        String k = "k";
        assertNull(store().get(k));
        store().put(k, k);
        assertEquals(k, store().get(k));
        store().delete(k);
        assertNull(store().get(k));
    }

}
