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
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;

public class MockStreaming extends KafkaStreaming {

    public MockStreaming(StreamingConfig config, Map<Integer, List<TopicPartition>> assignment) {
        super(config, 
              new MockProducer<byte[], byte[]>(), 
              new MockConsumer<byte[], byte[]>());
        super.rebalanceCallback.onPartitionsRevoked(mockConsumer(), Collections.<Integer, List<TopicPartition>>emptyMap());
        super.rebalanceCallback.onPartitionsAssigned(mockConsumer(), assignment);
    }
    
    @SuppressWarnings("rawtypes")
    public MockProducer mockProducer() {
        return (MockProducer) super.producer;
    }
    
    @SuppressWarnings("rawtypes")
    public MockConsumer mockConsumer() {
        return (MockConsumer) super.consumer;
    }
    
    @Override
    protected Consumer<byte[], byte[]> createConsumer() {
        return new MockConsumer<byte[], byte[]>();
    }

}
