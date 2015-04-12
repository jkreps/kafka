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

import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RoundRobinChooser<K, V> implements Chooser<K, V> {
    
    private Deque<ConsumerRecord<K, V>> q = new ArrayDeque<ConsumerRecord<K, V>>();

    @Override
    public void add(ConsumerRecord<K, V> record) {
        q.add(record);
    }

    @Override
    public ConsumerRecord<K, V> next() {
        return q.poll();
    }

    @Override
    public void close() {}

}
