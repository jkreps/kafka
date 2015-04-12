/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.streaming.internals;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.streaming.Chooser;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * A wrapper around a Kafka consumer that prioritizes record consumption from partitions using a
 * {@link org.apache.kafka.clients.streaming.Chooser}
 */
public class ChoosyConsumer<K, V> {

    private final Consumer<byte[], byte[]> consumer;
    private final Chooser<K, V> chooser;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final Map<TopicPartition, Deque<ConsumerRecord<K, V>>> stash;
    private final int desiredUnprocessed;
    private final long pollTimeMs;
    private int buffered;

    public ChoosyConsumer(Consumer<byte[], byte[]> consumer,
                          Chooser<K, V> chooser,
                          Deserializer<K> keyDeserializer,
                          Deserializer<V> valueDeserializer,
                          Metrics metrics,
                          int desiredNumberOfUnprocessedRecords,
                          long pollTimeMs) {
        this.consumer = consumer;
        this.chooser = chooser;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.stash = new HashMap<TopicPartition, Deque<ConsumerRecord<K, V>>>();
        this.desiredUnprocessed = desiredNumberOfUnprocessedRecords;
        this.pollTimeMs = pollTimeMs;
        this.buffered = 0;
        setupMetrics(metrics);
    }

    private void setupMetrics(Metrics metrics) {
        String group = "kafka-streaming";
        MetricName name = new MetricName("buffered-records", group, "The number of records currently buffered.");
        metrics.addMetric(name, new Measurable() {
            public double measure(MetricConfig config, long now) {
                return buffered;
            }
        });
    }

    public void init() {}

    public ConsumerRecord<K, V> next() {
        ConsumerRecord<K, V> record = chooser.next();

        if (record == null) {
            // no records available, poll until one arrives
            poll(this.pollTimeMs);
        } else {
            // we got a record: update the chooser and maybe initiate new fetch requests
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            Deque<ConsumerRecord<K, V>> recs = stash.get(partition);
            ConsumerRecord<K, V> next = recs.poll();
            if (next != null)
                chooser.add(next);
            if (recs.size() <= this.desiredUnprocessed) {
                // this partition is no longer full, initiate a new fetch without blocking
                consumer.unpause(partition);
                poll(0);
            }
            this.buffered--;
        }

        return record;
    }

    private void poll(long timeoutMs) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(timeoutMs);
        for (TopicPartition partition : records.partitions())
            refillPartition(partition, records.records(partition));
    }

    private void refillPartition(TopicPartition partition, Iterable<ConsumerRecord<byte[], byte[]>> records) {
        Deque<ConsumerRecord<K, V>> recs = stash.get(partition);
        if (recs == null) {
            recs = new ArrayDeque<ConsumerRecord<K, V>>();
            this.stash.put(partition, recs);
        }
        for (ConsumerRecord<byte[], byte[]> record : records) {
            K key = keyDeserializer.deserialize(record.topic(), record.key());
            V value = valueDeserializer.deserialize(record.topic(), record.value());
            recs.add(new ConsumerRecord<K, V>(record.topic(), record.partition(), record.offset(), key, value));
            this.buffered++;
        }

        // if we have buffered enough for this partition, pause
        if (recs.size() > this.desiredUnprocessed) {
            consumer.pause(partition);
        }
    }

    public void clear() {
        this.stash.clear();
        this.buffered = 0;
    }

}
