package org.apache.kafka.clients.streaming;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.streaming.kv.InMemoryKeyValueStore;
import org.apache.kafka.clients.streaming.kv.KeyValueIterator;
import org.apache.kafka.clients.streaming.kv.KeyValueStore;
import org.apache.kafka.common.Entry;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Run a test job that counts the occurances of each letter and validate the tests
 */
public class KafkaStreamingTest {
    
    private static String TOPIC = "test";
    private static int RECORDS_PER_LETTER = 20;
    private static int PARTITIONS = 5;
        
    @Test
    public void simpleTest() {
        for(int iteration = 1; iteration < 10; iteration++) {
            runJob(iteration);
        }
    }
    
    private void runJob(int iteration) {
        Properties props = new Properties();
        props.setProperty(StreamingConfig.TOTAL_RECORDS_TO_PROCESS, Integer.toString(RECORDS_PER_LETTER * TestUtils.LETTERS.length()));
        props.setProperty("iteration", Integer.toString(iteration));
        StreamingConfig config = new StreamingConfig(props);
        config.processor(TestProcessor.class);
        config.subscribe(TOPIC);
        config.chooser(new RoundRobinChooser<String, String>());
        config.serialization(new StringSerializer(), new StringDeserializer());
        Map<Integer, List<TopicPartition>> assignment = new HashMap<Integer, List<TopicPartition>>();
        for(int i = 0; i < PARTITIONS; i++)
            assignment.put(i, Collections.singletonList(new TopicPartition(TOPIC, i)));
        MockStreaming streaming = new MockStreaming(config, assignment);
        for(int i = 0; i < RECORDS_PER_LETTER; i++) {
            for(int j = 0; j < TestUtils.LETTERS.length(); j++) {
                byte[] letter = Character.toString(TestUtils.LETTERS.charAt(j)).getBytes();
                streaming.mockConsumer().addRecord(new ConsumerRecord<byte[], byte[]>(TOPIC, i % PARTITIONS, j, letter, letter));
            }
        }
        streaming.run();
    }
    
    public static class TestProcessor implements StreamProcessor<String, String>, StreamProcessor.Initializable, StreamProcessor.Closeable {
        private KeyValueStore<String, String> store;
        private int iteration;
        
        public TestProcessor() {}
        
        @Override
        public void init(ProcessorContext context, StorageManager storage) {
            this.store = new InMemoryKeyValueStore<String, String>("test-store", context);
            storage.registerAndRestore(this.store);
            this.iteration = Integer.parseInt(context.config().config().getProperty("iteration"));
            validateStore((iteration - 1) * RECORDS_PER_LETTER);
        }
        
        @Override
        public void process(ConsumerRecord<String, String> record,
                            RecordCollector<String, String> collector,
                            Coordinator coordinator) throws Exception {
            collector.send(new ProducerRecord<String, String>(record.topic(), record.key(), record.value()));
            String found = store.get(record.key());
            int count = found == null? 0 : Integer.parseInt(found);
            store.put(record.key(), Integer.toString(count + 1));
        }
        
        @Override
        public void close() {
            validateStore(iteration * RECORDS_PER_LETTER);
        }
        
        private void validateStore(int count) {
            KeyValueIterator<String, String> iter = store.all();
            int i = 0;
            while(iter.hasNext()) {
                Entry<String, String> entry = iter.next();
                String expected = Character.toString(TestUtils.LETTERS.charAt(i));
                assertEquals(expected, entry.key());
                assertEquals(count, Integer.parseInt(entry.value()));
                i++;
            }
        }
    }
    
}
