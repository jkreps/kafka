/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kafka.clients.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Implement this interface to specify the logic for processing a stream of records coming from one or more topics
 *  and producing a stream of output records to zero or more output topics.
 *  
 * This interface specifies only a single method for processing one record and perhaps producing some output.
 *  
 * Implementations that want to specify initialization logic can also implement {@link Initializable}; implementations that want special 
 * shutdown behavior can implement {@link Closeable}.
 *  
 * @param <K> The key type
 * @param <V> The value type
 */
public interface StreamProcessor<K, V> {
    
    /**
     * Process a single record. This may make changes to the internal state of the processor as well as 
     * produce some output using the provided {@link RecordCollector}.
     * @param record The record to process
     * @param collector The collector used to send output
     * @param coordinator A coordination interface used to manually request commit or shutdown
     * @throws Exception If any uncaught exception is thrown during processing that will shutdown all processing
     */
    public void process(ConsumerRecord<K, V> record, RecordCollector<K, V> collector, Coordinator coordinator) throws Exception;
    
    /**
     * By implementing this interface the stream processor can specify special logic to be run when the processor is initialized.
     */
    public interface Initializable {
        public void init(ProcessorContext context, StorageManager storage) throws Exception;
    }
    
    /**
     * By implementing this inteface the stream processor can specify special logic to be run when the processor is closed.
     */
    public interface Closeable {
        public void close() throws Exception;
    }
    
}
