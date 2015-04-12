/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.streaming;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.streaming.kv.InMemoryKeyValueStore;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ExampleStreamProcessor implements StreamProcessor<String, String>, StreamProcessor.Initializable {

    private StorageEngine engine;

    public void init(ProcessorContext context, StorageManager storage) {
        System.out.println("ID: " + context.id());
        this.engine = new InMemoryKeyValueStore<String, String>("changelog", context);
        storage.registerAndRestore(engine);
    }

    @Override
    public void process(ConsumerRecord<String, String> record,
                        RecordCollector<String, String> collector,
                        Coordinator coordinator) throws Exception {
        System.out.println(record);
        if (record.topic().equals("test"))
            collector.send(new ProducerRecord<String, String>("test2", record.key(), record.value()));
    }

    public static void main(String[] args) throws Exception {
        // read properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "my-group");
        StreamingConfig config = new StreamingConfig(props);
        config.subscribe("test");
        config.addContextObject("my-object", "test-value");
        config.processor(ExampleStreamProcessor.class);
        config.serialization(new StringSerializer(), new StringDeserializer());
        KafkaStreaming container = new KafkaStreaming(config);
        container.run();
    }

}
