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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.streaming.RecordCollector;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaRecordCollector<K, V> implements RecordCollector<K, V> {

    private final Producer<byte[], byte[]> producer;
    private final Serializer<Object> keySerializer;
    private final Serializer<Object> valueSerializer;
    private final Map<TopicPartition, Long> offsets;
    private final Callback callback = new Callback() {
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null)
                offsets.put(new TopicPartition(metadata.topic(), metadata.partition()), metadata.offset());
        }
    };
    
    public KafkaRecordCollector(Producer<byte[], byte[]> producer, Serializer<Object> serializer, Serializer<Object> serializer2) {
        this.producer = producer;
        this.offsets = new HashMap<TopicPartition, Long>();
        this.keySerializer = serializer;
        this.valueSerializer = serializer2;
    }

    @Override
    public void send(ProducerRecord<K, V> record) {
        byte[] keyBytes = keySerializer.serialize(record.topic(), record.key());
        byte[] valBytes = valueSerializer.serialize(record.topic(), record.value());
        // TODO: need to compute partition
        this.producer.send(new ProducerRecord<byte[], byte[]>(record.topic(), keyBytes, valBytes), callback);
    }

    public Producer<byte[], byte[]> producer() {
        return this.producer;
    }
    
    public Map<TopicPartition, Long> offsets() {
        return this.offsets;
    }

}
