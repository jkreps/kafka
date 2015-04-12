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

package org.apache.kafka.clients.streaming.internals;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.streaming.Coordinator;
import org.apache.kafka.clients.streaming.ProcessorContext;
import org.apache.kafka.clients.streaming.RecordCollector;
import org.apache.kafka.clients.streaming.StorageEngine;
import org.apache.kafka.clients.streaming.StorageManager;
import org.apache.kafka.clients.streaming.StreamProcessor;
import org.apache.kafka.clients.streaming.StreamingConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamProcessorInstance {

    private static final Logger log = LoggerFactory.getLogger(StreamProcessorInstance.class);

    private final StreamProcessor<Object, Object> processor;
    private final SimpleRecordCollector collector;
    private final RecordCollector<Object, Object> serializedCollector;
    private final Coordinator coordinator;
    private final ProcessorContext context;
    private final ProcessorStateManager state;
    private final Metrics metrics;
    private final Map<TopicPartition, Long> consumedOffsets;

    @SuppressWarnings("unchecked")
    public StreamProcessorInstance(int id,
                                   Class<?> processorClass,
                                   StreamingConfig config,
                                   File stateDir,
                                   Producer<byte[], byte[]> producer,
                                   Coordinator coordinator,
                                   Set<TopicPartition> partitions,
                                   Metrics metrics) {
        this.processor = (StreamProcessor<Object, Object>) Utils.newInstance(processorClass);
        this.coordinator = coordinator;
        this.collector = new SimpleRecordCollector(producer);
        this.serializedCollector = new SerializingRecordCollector<Object, Object>(this.collector, (Serializer<Object>) config.keySerializer(), (Serializer<Object>) config.valueSerializer());
        this.context = new ProcessorContext(id, config, stateDir, partitions, metrics);
        this.metrics = metrics;
        this.state = new ProcessorStateManager(id, stateDir);
        this.consumedOffsets = new HashMap<TopicPartition, Long>();
    }

    public void init(final Consumer<byte[], byte[]> consumer) throws Exception {
        if (this.processor instanceof StreamProcessor.Initializable) {
            StreamProcessor.Initializable initable = ((StreamProcessor.Initializable) this.processor);
            initable.init(context, new StorageManager() {
                @Override
                public void registerAndRestore(StorageEngine engine) {
                    state.registerAndRestore(collector, consumer, engine);
                }     
            });
        }
    }

    public void process(ConsumerRecord<Object, Object> record) throws Exception {
        this.processor.process(record, this.serializedCollector, coordinator);
        if (record != null)
            this.consumedOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset());
    }

    public void window() throws Exception {
        process(null);
    }

    public void flush() {
        this.state.flush();
    }

    public void close() throws Exception {
        if (this.processor instanceof StreamProcessor.Closeable)
            ((StreamProcessor.Closeable) this.processor).close();
        this.state.close(this.collector.offsets());
    }

    public Map<TopicPartition, Long> consumedOffsets() {
        return this.consumedOffsets;
    }
    
    private static class SimpleRecordCollector implements RecordCollector<byte[], byte[]> {

        private final Producer<byte[], byte[]> producer;
        private final Map<TopicPartition, Long> offsets;
        private final Callback callback = new Callback(){
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    TopicPartition tp = new TopicPartition(metadata.topic(), metadata.partition());
                    offsets.put(tp, metadata.offset());
                } else {
                    log.error("Error sending record: ", exception);
                }
            }
        };
        
        public SimpleRecordCollector(Producer<byte[], byte[]> producer) {
            this.producer = producer;
            this.offsets = new HashMap<TopicPartition, Long>();
        }

        @Override
        public void send(ProducerRecord<byte[], byte[]> record) {
            // TODO: need to compute partition
            this.producer.send(record, callback);
        }
        
        /**
         * The last ack'd offset from the producer
         */
        public Map<TopicPartition, Long> offsets() {
            return this.offsets;
        }
    }
    
    private static class SerializingRecordCollector<K, V> implements RecordCollector<K, V> {
        
        private final RecordCollector<byte[], byte[]> collector;
        private final Serializer<K> keySerializer;
        private final Serializer<V> valueSerializer;
        
        public SerializingRecordCollector(RecordCollector<byte[], byte[]> collector, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
            super();
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.collector = collector;
        }

        @Override
        public void send(ProducerRecord<K, V> record) {
            byte[] keyBytes = keySerializer.serialize(record.topic(), record.key());
            byte[] valBytes = valueSerializer.serialize(record.topic(), record.value());
            collector.send(new ProducerRecord<byte[], byte[]>(record.topic(), keyBytes, valBytes));
        }
    }

}
