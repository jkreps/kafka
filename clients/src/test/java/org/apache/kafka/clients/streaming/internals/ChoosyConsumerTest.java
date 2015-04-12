package org.apache.kafka.clients.streaming.internals;

import static org.junit.Assert.*;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.streaming.RoundRobinChooser;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

public class ChoosyConsumerTest {
    
    private final StringDeserializer deserializer = new StringDeserializer();
    private final StringSerializer serializer = new StringSerializer();
    private MockConsumer<byte[], byte[]> mock;
    private RoundRobinChooser<String, String> chooser;
    private ChoosyConsumer<String, String> consumer;
    private int desiredUnprocessed = 10;
    
    @Before
    public void setup() {
        mock = new MockConsumer<byte[], byte[]>();
        chooser = new RoundRobinChooser<String, String>();
        consumer = new ChoosyConsumer<String, String>(mock, chooser, deserializer, deserializer, new Metrics(), desiredUnprocessed, 10L);
    }
    
    @Test
    public void testConsumer() {
        send("topic-a", 0, 0, 5); // send 5 records to topic-a
        send("topic-b", 0, 0, 5); // send 5 records to topic-b
        consumer.init();
        send("topic-a", 0, 5, 10); // send 5 more records to topic-a
        send("topic-b", 0, 5, 10); // send 5 more records to topic-b
        // check that they round robin
        String t1 = consumer.next().topic();
        String t2 = consumer.next().topic();
        assertFalse("Should get one record from each topic", t1.equals(t2));
        for(int i = 2; i < 20; i++) {
            ConsumerRecord<String, String> found = consumer.next();
            String topic = i % 2 == 0? t1 : t2;
            String kv = Integer.toString(i / 2);
            ConsumerRecord<Object, Object> expected = new ConsumerRecord<Object, Object>(topic, 0, i / 2, kv, kv);
            assertEquals(expected, found);
        }
        assertNull("No more records should be left", consumer.next());
    }
    
    private void send(String topic, int partition, int from, int to) {
        for(int i = from; i < to; i++)
            mock.addRecord(record(topic, partition, i, i));
    }
    
    private ConsumerRecord<byte[], byte[]> record(String topic, int partition, long offset, int kv) {
        byte[] bytes = serializer.serialize(topic, Integer.toString(kv));
        return new ConsumerRecord<byte[], byte[]>(topic, partition, offset, bytes, bytes);
    }

}
