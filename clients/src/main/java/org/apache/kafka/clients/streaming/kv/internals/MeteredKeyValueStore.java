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

package org.apache.kafka.clients.streaming.kv.internals;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.streaming.RecordCollector;
import org.apache.kafka.clients.streaming.kv.KeyValueIterator;
import org.apache.kafka.clients.streaming.kv.KeyValueStore;
import org.apache.kafka.common.Entry;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;

public class MeteredKeyValueStore<K, V> implements KeyValueStore<K, V> {

    protected final KeyValueStore<K, V> inner;
    private final Time time;
    private final Metrics metrics;
    private final String group;
    private final Sensor putTime;
    private final Sensor getTime;
    private final Sensor deleteTime;
    private final Sensor putAllTime;
    private final Sensor allTime;
    private final Sensor rangeTime;
    private final Sensor flushTime;
    private final Sensor restoreTime;
    
    public MeteredKeyValueStore(String name, String group, KeyValueStore<K, V> inner, Metrics metrics, Time time) {
        this.inner = inner;
        this.time = time;
        this.metrics = metrics;
        this.group = group;
        this.putTime = createSensor(name, "put");
        this.getTime = createSensor(name, "get");
        this.deleteTime = createSensor(name, "delete");
        this.putAllTime = createSensor(name, "put-all");
        this.allTime = createSensor(name, "all");
        this.rangeTime = createSensor(name, "range");
        this.flushTime = createSensor(name, "flush");
        this.restoreTime = createSensor(name, "restore");
    }
    
    private Sensor createSensor(String storeName, String operation) {
        Sensor parent = metrics.sensor(operation);
        addLatencyMetrics(parent, operation);
        Sensor sensor = metrics.sensor(storeName + "- " + operation, parent);
        addLatencyMetrics(sensor, operation, "store-name", storeName);
        return sensor;
    }
    
    private void addLatencyMetrics(Sensor sensor, String opName, String... kvs) {
        maybeAddMetric(sensor, new MetricName(opName + "-avg-latency-ms", group, "The average latency in milliseconds of the key-value store operation.", kvs), new Avg());
        maybeAddMetric(sensor, new MetricName(opName + "-max-latency-ms", group, "The max latency in milliseconds of the key-value store operation.", kvs), new Max());
        maybeAddMetric(sensor, new MetricName(opName + "-qps", group, "The average number of occurance of the given key-value store operation per second.", kvs), new Rate(new Count()));
    }
    
    private void maybeAddMetric(Sensor sensor, MetricName name, MeasurableStat stat) {
        if (!metrics.metrics().containsKey(name))
            sensor.add(name, stat);
    }
    
    @Override
    public String name() {
        return inner.name();
    }

    @Override
    public void registerAndRestore(RecordCollector<byte[], byte[]> collector,
                                   Consumer<byte[], byte[]> consumer,
                                   TopicPartition partition,
                                   long checkpointedOffset,
                                   long logEndOffset) {
        long startNs = time.nanoseconds();
        try {
            inner.registerAndRestore(collector, consumer, partition, checkpointedOffset, logEndOffset);
        } finally {
            recordLatency(this.restoreTime, startNs, time.nanoseconds());
        }
    }

    @Override
    public V get(K key) {
        long startNs = time.nanoseconds();
        try {
            return this.inner.get(key);
        } finally {
            recordLatency(this.getTime, startNs, time.nanoseconds());
        }
    }
    
    @Override
    public void put(K key, V value) {
        long startNs = time.nanoseconds();
        try {
            this.inner.put(key, value);
        } finally {
            recordLatency(this.putTime, startNs, time.nanoseconds());
        } 
    }
    
    @Override
    public void putAll(List<Entry<K, V>> entries) {
        long startNs = time.nanoseconds();
        try {
            this.inner.putAll(entries);
        } finally {
            recordLatency(this.putAllTime, startNs, time.nanoseconds());
        } 
    }
    
    @Override
    public void delete(K key) {
        long startNs = time.nanoseconds();
        try {
            this.inner.delete(key);
        } finally {
            recordLatency(this.deleteTime, startNs, time.nanoseconds());
        } 
    }
    
    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return new MeteredKeyValueIterator<K, V>(this.inner.range(from, to), this.rangeTime);
    }
    
    @Override
    public KeyValueIterator<K, V> all() {
        return new MeteredKeyValueIterator<K, V>(this.inner.all(), this.allTime);
    }
    
    @Override
    public void close() {}
    
    @Override
    public void flush() {
        long startNs = time.nanoseconds();
        try {
            this.inner.flush();
        } finally {
            recordLatency(this.flushTime, startNs, time.nanoseconds());
        } 
    }
    
    private void recordLatency(Sensor sensor, long startNs, long endNs) {
        sensor.record((endNs - startNs) / Time.NS_PER_MS, endNs);
    }
    
    private class MeteredKeyValueIterator<K1, V1> implements KeyValueIterator<K1, V1> {
        
        private final KeyValueIterator<K1, V1> iter;
        private final Sensor sensor;
        private final long startNs;

        public MeteredKeyValueIterator(KeyValueIterator<K1, V1> iter, Sensor sensor) {
            this.iter = iter;
            this.sensor = sensor;
            this.startNs = time.nanoseconds();
        }
        
        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Entry<K1, V1> next() {
            return iter.next();
        }

        @Override
        public void remove() {
            iter.remove();
        }

        @Override
        public void close() {
            try {
                iter.close();
            } finally {
                recordLatency(this.sensor, this.startNs, time.nanoseconds());
            }
        }
            
    }
    
}
