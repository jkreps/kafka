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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Configuration information passed to the {@link KafkaStreaming} instance for configuring the associated
 * {@link org.apache.kafka.clients.producer.KafkaProducer KafkaProducer}, and 
 * {@link org.apache.kafka.clients.consumer.KafkaConsumer KafkaConsumer}, as
 * well as the processor itself.
 */
public class StreamingConfig {
    
    /** <code>window.time.ms</code> */
    public static final String STATE_DIR_CONFIG = "state.dir";
    
    /** <code>poll.time.ms</code> */
    public static final String POLL_TIME_MS_CONFIG = "poll.time.ms";
    
    /** <code>commit.time.ms</code> */
    public static final String COMMIT_TIME_MS_CONFIG = "commit.time.ms";
    
    /** <code>window.time.ms</code> */
    public static final String WINDOW_TIME_MS_CONFIG = "window.time.ms";
    
    /** <code>buffered.records.per.partition</code> */
    public static final String BUFFERED_RECORDS_PER_PARTITION_CONFIG = "buffered.records.per.partition";

    /** <code>state.cleanup.delay</code> */
    public static final String STATE_CLEANUP_DELAY_CONFIG = "state.cleanup.delay";
    
    /** <code>total.records.to.process</code> */
    public static final String TOTAL_RECORDS_TO_PROCESS = "total.records.to.process";
    
    private final Properties config;
    private final Map<String, Object> context = new HashMap<String, Object>();
    private final Set<String> topics = new HashSet<String>();
    private Class<?> processor;
    private Chooser<Object, Object> chooser = new RoundRobinChooser<Object, Object>();
    private Serializer<?> keySerializer;
    private Serializer<?> valSerializer;
    private Deserializer<?> keyDeserializer;
    private Deserializer<?> valDeserializer;
    
    public StreamingConfig(Properties config) {
        this.config = config;
        config.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.setProperty(ProducerConfig.LINGER_MS_CONFIG, "100");
    }
    
    public void addContextObject(String key, Object value) {
        this.context.put(key, value);
    }
    
    @SuppressWarnings("all")
    public void chooser(Chooser chooser) {
        this.chooser = chooser;
    }
    
    public void subscribe(String...topic) {
        Collections.addAll(this.topics, topic);
    }
    
    public void processor(Class<?> c) {
        this.processor = c;
    }
    
    public void serialization(Serializer<?> serializer, Deserializer<?> deserializer) {
        keySerializer(serializer);
        valueSerializer(serializer);
        keyDeserializer(deserializer);
        valueDeserializer(deserializer);
    }
    
    public void keySerializer(Serializer<?> serializer) {
        this.keySerializer = serializer;
    }
    
    public void valueSerializer(Serializer<?> serializer) {
        this.valSerializer = serializer;
    }
    
    public void keyDeserializer(Deserializer<?> deserializer) {
        this.keyDeserializer = deserializer;
    }
    
    public void valueDeserializer(Deserializer<?> deserializer) {
        this.valDeserializer = deserializer;
    }
    
    public Chooser<Object, Object> chooser() {
        return this.chooser;
    }
    
    public Properties config() {
        return this.config;
    }
    
    public Map<String, Object> context() {
        return this.context;
    }
    
    public Class<?> processor() {
        // TODO: need to validate these
        return this.processor;
    }
    
    public Set<String> topics() {
        return this.topics;
    }
    
    public Serializer<?> keySerializer() {
        return this.keySerializer;
    }
    
    public Serializer<?> valueSerializer() {
        return this.valSerializer;
    }
    
    public Deserializer<?> keyDeserializer() {
        return this.keyDeserializer;
    }
    
    public Deserializer<?> valueDeserializer() {
        return this.valDeserializer;
    }
}
