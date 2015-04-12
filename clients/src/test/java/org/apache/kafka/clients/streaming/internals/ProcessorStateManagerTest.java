package org.apache.kafka.clients.streaming.internals;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.streaming.ProcessorContext;
import org.apache.kafka.clients.streaming.RecordCollector;
import org.apache.kafka.clients.streaming.StorageEngine;
import org.apache.kafka.clients.streaming.StreamingConfig;
import org.apache.kafka.clients.streaming.kv.InMemoryKeyValueStore;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ProcessorStateManagerTest {
    
    private final Time time = new MockTime();
    private final String topic = "test";
    private final int partition = 0;
    private ProcessorContext context;
    private List<String> letters;
    private File stateDir;
    private MockConsumer<byte[], byte[]> consumer;
    private MockProducer<byte[], byte[]> producer;
    private RecordCollector<byte[], byte[]> collector;
    
    @Before
    public void setup() {
        this.stateDir = TestUtils.tempDir();
        this.letters = new ArrayList<String>();
        for(int i = 0; i < TestUtils.LETTERS.length(); i++)
            letters.add(TestUtils.LETTERS.substring(i, i + 1));
        StreamingConfig config = new StreamingConfig(new Properties());
        config.serialization(new StringSerializer(), new StringDeserializer());
        this.context = new ProcessorContext(partition,
                                            config, 
                                            stateDir, 
                                            new HashSet<TopicPartition>(), 
                                            new Metrics());
        this.producer = new MockProducer<byte[], byte[]>();
        this.collector = new RecordCollector<byte[], byte[]>() {
            public void send(ProducerRecord<byte[], byte[]> record) {
                producer.send(record);
            }
        };
    }
    
    @After
    public void teardown() {
        Utils.rm(stateDir);
    }
    
    @Test
    public void testRestore() throws Exception {
        int iterations = 500;
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>();
        InMemoryKeyValueStore<String, String> store = new InMemoryKeyValueStore<String, String>("blah", context);
        ProcessorStateManager stateManager = new ProcessorStateManager(0, stateDir);
        stateManager.registerAndRestore(collector, consumer, store);
        
        // write some records
        for(int i = 0; i < iterations; i++) {
            String v = Integer.toString(i);
            store.put(v, v);
        }
        stateManager.flush();
        stateManager.close(new HashMap<TopicPartition, Long>());
        
        // take what was sent and put it in the consumer
        long offset = 0;
        for(ProducerRecord<byte[], byte[]> record: this.producer.history())
            consumer.addRecord(new ConsumerRecord<byte[], byte[]>(record.topic(), partition, offset++, record.key(), record.value()));
        
        ProcessorStateManager stateManager2 = new ProcessorStateManager(0, stateDir);
        InMemoryKeyValueStore<String, String> store2 = new InMemoryKeyValueStore<String, String>("blah", context);
        stateManager2.registerAndRestore(collector, consumer, store2);
        for(int i = 0; i < iterations; i++) {
            String v = Integer.toString(i);
            assertEquals(v, store2.get(v));
        }

    }
    
}
