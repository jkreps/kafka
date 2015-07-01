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

package org.apache.kafka.clients.streaming.kv;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.streaming.ProcessorContext;
import org.apache.kafka.clients.streaming.RecordCollector;
import org.apache.kafka.clients.streaming.StorageEngine;
import org.apache.kafka.clients.streaming.kv.internals.MeteredKeyValueStore;
import org.apache.kafka.common.Entry;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.SystemTime;

/**
 * An in-memory key-value store based on a TreeMap
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public class InMemoryKeyValueStore<K, V> extends MeteredKeyValueStore<K, V> implements KeyValueStore<K, V>, StorageEngine {
    
    public InMemoryKeyValueStore(String name, ProcessorContext context) {
        super(name, "kafka-streams", new MemoryStore<K, V>(name, context), context.metrics(), new SystemTime());
    }

    private static class MemoryStore<K, V> implements KeyValueStore<K, V>, StorageEngine {

        private final String topic;
        private final int partition;
        private final NavigableMap<K, V> store;
        private final Set<K> dirty;
        private final int maxDirty;
        private final Serializer<K> keySerializer;
        private final Serializer<V> valueSerializer;
        private final Deserializer<K> keyDeserializer;
        private final Deserializer<V> valueDeserializer;
        private RecordCollector<byte[], byte[]> collector;

        public MemoryStore(String name, ProcessorContext context) {
            this.topic = name;
            this.partition = context.id();
            this.store = new TreeMap<K, V>();
            this.dirty = new HashSet<K>();
            this.collector = null;
            this.maxDirty = 100;
            this.keySerializer = (Serializer<K>) context.config().keySerializer();
            this.valueSerializer = (Serializer<V>) context.config().valueSerializer();
            this.keyDeserializer = (Deserializer<K>) context.config().keyDeserializer();
            this.valueDeserializer = (Deserializer<V>) context.config().valueDeserializer();
        }
        
        @Override
        public String name() {
            return this.topic;
        }

        @Override
        public V get(K key) {
            return this.store.get(key);
        }

        @Override
        public void put(K key, V value) {
            this.store.put(key, value);
            if (this.collector != null) {
                this.dirty.add(key);
                if (this.dirty.size() > this.maxDirty)
                    flush();
            }
        }

        @Override
        public void putAll(List<Entry<K, V>> entries) {
            for (Entry<K, V> entry : entries)
                put(entry.key(), entry.value());
        }

        @Override
        public void delete(K key) {
            put(key, null);
        }

        @Override
        public KeyValueIterator<K, V> range(K from, K to) {
            return new MemoryStoreIterator<K, V>(this.store.subMap(from, true, to, false).entrySet().iterator());
        }

        @Override
        public KeyValueIterator<K, V> all() {
            return new MemoryStoreIterator<K, V>(this.store.entrySet().iterator());
        }

        @Override
        public void registerAndRestore(RecordCollector<byte[], byte[]> collector, 
                                       Consumer<byte[], byte[]> consumer, 
                                       TopicPartition partition, 
                                       long checkpointedOffset, 
                                       long endOffset) {
            this.collector = collector;
            while (true) {
                for (ConsumerRecord<byte[], byte[]> record: consumer.poll(100))
                    this.store.put(keyDeserializer.deserialize(partition.topic(), record.key()),
                                   valueDeserializer.deserialize(partition.topic(), record.value()));
                long position = consumer.position(partition);
                if (position == endOffset)
                    break;
                else if (position > endOffset)
                    throw new IllegalStateException("This should not happen.");
            }
        }

        @Override
        public void flush() {
            if (this.collector != null) {
                for (K k : this.dirty) {
                    V v = this.store.get(k);
                    byte[] key = this.keySerializer.serialize(this.topic, k);
                    byte[] value = this.valueSerializer.serialize(this.topic, v);
                    this.collector.send(new ProducerRecord<byte[], byte[]>(this.topic, this.partition, key, value));
                }
                this.dirty.clear();
            }
        }

        @Override
        public void close() {
            flush();
        }

        private static class MemoryStoreIterator<K, V> implements KeyValueIterator<K, V> {
            private final Iterator<Map.Entry<K, V>> iter;

            public MemoryStoreIterator(Iterator<Map.Entry<K, V>> iter) {
                this.iter = iter;
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public Entry<K, V> next() {
                Map.Entry<K, V> entry = iter.next();
                return new Entry<K, V>(entry.getKey(), entry.getValue());
            }

            @Override
            public void remove() {
                iter.remove();
            }

            @Override
            public void close() {}

        }
    }

}
