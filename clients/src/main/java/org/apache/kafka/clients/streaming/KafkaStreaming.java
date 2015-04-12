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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.CommitType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceCallback;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.streaming.internals.ChoosyConsumer;
import org.apache.kafka.clients.streaming.internals.ProcessorConfig;
import org.apache.kafka.clients.streaming.internals.StreamProcessorInstance;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streaming allows for performing continuous computation on input coming from one or more input topics and
 * sends output to zero or more output topics.
 * <p>
 * This processing is done by implementing the {@link StreamProcessor} interface to specify the transformation. The
 * {@link KafkaStreaming} instance will be responsible for the lifecycle of these processors. It will instantiate and
 * start one or more of these processors to process the Kafka partitions assigned to this particular instance.
 * <p>
 * This streaming instance will co-ordinate with any other instances (whether in this same process, on other processes 
 * on this machine, or on remote machines). These processes will divide up the work so that all partitions are being 
 * consumed. If instances are added or die, the corresponding {@link StreamProcessor} instances will be shutdown or
 * started in the appropriate processes to balance processing load.
 * <p>
 * Internally the {@link KafkaStreaming} instance contains a normal {@link org.apache.kafka.clients.producer.KafkaProducer KafkaProducer}
 * and {@link org.apache.kafka.clients.consumer.KafkaConsumer KafkaConsumer} instance that is used for reading input and writing output.
 * <p>
 * A simple example might look like this:
 * <pre>
 *    Properties props = new Properties();
 *    props.put("bootstrap.servers", "localhost:4242");
 *    StreamingConfig config = new StreamingConfig(props);
 *    config.subscribe("test-topic-1", "test-topic-2");
 *    config.processor(ExampleStreamProcessor.class);
 *    config.serialization(new StringSerializer(), new StringDeserializer());
 *    KafkaStreaming container = new KafkaStreaming(config);
 *    container.run();
 * </pre>
 * 
 */
public class KafkaStreaming implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreaming.class);

    private final Map<Integer, StreamProcessorInstance> processors;
    private final Map<TopicPartition, StreamProcessorInstance> processorsByPartition;
    protected final Producer<byte[], byte[]> producer;
    protected final Consumer<byte[], byte[]> consumer;
    private final ChoosyConsumer<Object, Object> choosyConsumer;
    private final StreamingConfig streamingConfig;
    private final ProcessorConfig config;
    private final Metrics metrics;
    private final KafkaStreamingMetrics streamingMetrics;
    private final Time time;
    private final List<Integer> requestingCommit;
    private volatile boolean running;
    private CountDownLatch shutdownComplete = new CountDownLatch(1);
    private long lastCommit;
    private long lastWindow;
    private long nextStateCleaning;
    private long recordsProcessed;

    protected final ConsumerRebalanceCallback rebalanceCallback = new ConsumerRebalanceCallback() {
        @Override
        public void onPartitionsAssigned(Consumer<?, ?> consumer, Map<Integer, List<TopicPartition>> assignment) {
            addProcessors(assignment);
        }

        @Override
        public void onPartitionsRevoked(Consumer<?, ?> consumer, Map<Integer, List<TopicPartition>> assignment) {
            closeProcessors(assignment);
        }
    };

    public KafkaStreaming(StreamingConfig config) {
        this(config, null, null);
    }
    
    protected KafkaStreaming(StreamingConfig config, 
                             Producer<byte[], byte[]> producer, 
                             Consumer<byte[], byte[]> consumer) {
        this.processors = new HashMap<Integer, StreamProcessorInstance>();
        this.processorsByPartition = new HashMap<TopicPartition, StreamProcessorInstance>();
        this.producer = producer == null ? new KafkaProducer<byte[], byte[]>(config.config(), new ByteArraySerializer(), new ByteArraySerializer()): producer;
        this.consumer = consumer == null ? new KafkaConsumer<byte[], byte[]>(config.config(), rebalanceCallback, new ByteArrayDeserializer(), new ByteArrayDeserializer()): consumer;
        this.streamingConfig = config;
        this.metrics = new Metrics();
        this.streamingMetrics = new KafkaStreamingMetrics();
        this.requestingCommit = new ArrayList<Integer>();
        this.config = new ProcessorConfig(config.config());
        this.choosyConsumer = 
                new ChoosyConsumer<Object, Object>(this.consumer, 
                                                   config.chooser(), 
                                                   (Deserializer<Object>) config.keyDeserializer(),
                                                   (Deserializer<Object>) config.valueDeserializer(),
                                                   this.metrics,
                                                   this.config.bufferedRecordsPerPartition, 
                                                   this.config.pollTimeMs);
        this.running = true;
        this.lastCommit = 0;
        this.lastWindow = 0;
        this.nextStateCleaning = Long.MAX_VALUE;
        this.recordsProcessed = 0;
        this.time = new SystemTime();
    }

    /**
     * Execute the stream processors
     */
    public synchronized void run() {
        init();
        try {
            runLoop();
        } catch (RuntimeException e) {
            log.error("Uncaught error during processing: ", e);
            throw e;
        } finally {
            shutdown();
        }
    }

    private void init() {
        log.info("Starting container");
        if (!this.running)
            throw new IllegalArgumentException("This container has already been shut down.");
        if (!config.stateDir.exists() && !config.stateDir.mkdirs())
            throw new IllegalArgumentException("Failed to create state directory: " + config.stateDir.getAbsolutePath());
        for (String topic: streamingConfig.topics())
            consumer.subscribe(topic);
        log.info("Start-up complete");
    }

    private void shutdown() {
        log.info("Shutting down container");
        commitAll(time.milliseconds());
        for (StreamProcessorInstance processor : this.processors.values()) {
            try {
                processor.close();
            } catch (Exception e) {
                log.error("Error while closing processor: ", e);
            }
        }
        producer.close();
        consumer.close();
        log.info("Shut down complete");
    }

    /**
     * Shutdown this streaming instance.
     */
    public synchronized void close() {
        this.running = false;
        try {
            this.shutdownComplete.await();
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    private void runLoop() {
        try {
            while (stillRunning()) {
                ConsumerRecord<Object, Object> record = this.choosyConsumer.next();
                if (record != null)
                    process(record);                         
                maybeWindow();
                maybeCommit(); 
                maybeCleanState();
            }
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }
    
    private boolean stillRunning() {
        if (!this.running) {
            log.debug("Shutting down at user request.");
            return false;
        }
        if (this.config.totalRecordsToProcess >= 0 && this.recordsProcessed >= this.config.totalRecordsToProcess) {
            log.debug("Shutting down as we've reached the user-configured limit of {} records to process.", this.config.totalRecordsToProcess);
            return false;
        }
        return true;
    }

    private void process(ConsumerRecord<Object, Object> record) throws Exception {
        long begin = time.milliseconds();
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        StreamProcessorInstance instance = this.processorsByPartition.get(tp);
        instance.process(record);
        this.streamingMetrics.processTime.record(time.milliseconds() - begin);
        this.recordsProcessed++;
    }

    private void maybeWindow() throws Exception {
        long now = time.milliseconds();
        if (this.config.windowTimeMs >= 0 && lastWindow + this.config.windowTimeMs < now) {
            log.trace("Windowing all stream processors");
            this.lastWindow = now;
            for (StreamProcessorInstance instance : processors.values()) {
                instance.window();
                // check co-ordinator
            }
            this.streamingMetrics.windowTime.record(time.milliseconds() - now);
        }
    }

    private void maybeCommit() {
        long now = time.milliseconds();
        if (this.config.commitTimeMs >= 0 && lastCommit + this.config.commitTimeMs < time.milliseconds()) {
            log.trace("Committing processor instances because the commit interval has elapsed.");
            commitAll(now);
        } else {
            if (!this.requestingCommit.isEmpty()) {
                log.trace("Committing processor instances because of user request.");
                commitRequesting(now);
            }
        }
    }
    
    private void commitAll(long now) {
        Map<TopicPartition, Long> commit = new HashMap<TopicPartition, Long>();
        for (StreamProcessorInstance processor : this.processors.values()) {
            processor.flush();
            commit.putAll(processor.consumedOffsets());
        }
        producer.flush();
        consumer.commit(commit, CommitType.SYNC); // TODO: can this be async?
        this.streamingMetrics.commitTime.record(time.milliseconds() - lastCommit);
    }
    
    private void commitRequesting(long now) {
        Map<TopicPartition, Long> commit = new HashMap<TopicPartition, Long>(this.requestingCommit.size());
        for (Integer id : this.requestingCommit) {
            StreamProcessorInstance processor = this.processors.get(id);
            processor.flush();
            commit.putAll(processor.consumedOffsets()); // TODO: can this be async?
        }
        consumer.commit(commit, CommitType.SYNC);
        this.requestingCommit.clear();
        this.streamingMetrics.commitTime.record(time.milliseconds() - now);
    }
    
    /* delete any state dirs that aren't for active processors */
    private void maybeCleanState() {
        long now = time.milliseconds();
        if (now > this.nextStateCleaning) {
            File[] stateDirs = this.config.stateDir.listFiles();
            if (stateDirs != null) {
                for (File dir: stateDirs) {
                    try {
                        Integer id = Integer.parseInt(dir.getName());
                        if (!this.processors.keySet().contains(id)) {
                            log.info("Deleting obsolete state directory {} after {} delay ms.", dir.getAbsolutePath(), config.stateCleanupDelay);
                            Utils.rm(dir);
                        }
                    } catch (NumberFormatException e) {
                        log.warn("Deleting unknown directory in state directory {}.", dir.getAbsolutePath());
                        Utils.rm(dir);
                    }
                }
            }
            this.nextStateCleaning = Long.MAX_VALUE;
        }
    }

    private void addProcessors(Map<Integer, List<TopicPartition>> assignment) {
        
        Consumer<byte[], byte[]> restoreConsumer = createConsumer();
        for (Map.Entry<Integer, List<TopicPartition>> entry: assignment.entrySet()) {
            final Integer id = entry.getKey();
            List<TopicPartition> partitions = entry.getValue();
            log.info("Adding processor {} for {}", id, partitions);
            File stateDir = new File(config.stateDir, id.toString());
            Coordinator coordinator = new Coordinator() {
                @Override
                public void commit(RequestScope scope) {
                    requestingCommit.add(id);
                }

                @Override
                public void shutdown(RequestScope scope) {
                    throw new IllegalStateException("Implement me");
                }
            };
            StreamProcessorInstance processor = new StreamProcessorInstance(id,
                                                                            streamingConfig.processor(),
                                                                            streamingConfig,
                                                                            stateDir,
                                                                            producer,
                                                                            coordinator,
                                                                            new HashSet<TopicPartition>(partitions),
                                                                            metrics);
            try {
                processor.init(restoreConsumer);
            } catch (Exception e) {
                throw new KafkaException(e);
            }
            processors.put(id, processor);
            for (TopicPartition p: partitions)
                processorsByPartition.put(p, processor);
            streamingMetrics.processorCreation.record();
        }
        restoreConsumer.close();
        this.choosyConsumer.init();
        this.nextStateCleaning = time.milliseconds() + config.stateCleanupDelay;
    }
    
    protected Consumer<byte[], byte[]> createConsumer() {
        return new KafkaConsumer<byte[], byte[]>(streamingConfig.config(), 
                                                 null, 
                                                 new ByteArrayDeserializer(),
                                                 new ByteArrayDeserializer());
    }

    private void closeProcessors(Map<Integer, List<TopicPartition>> assignment) {
        commitAll(time.milliseconds());
        // remove all partitions
        for (Map.Entry<Integer, List<TopicPartition>> entry: assignment.entrySet()) {
            Integer id = entry.getKey();
            log.info("Removing processor {}", id);
            processors.remove(id);
            for (TopicPartition partition: entry.getValue())
                processorsByPartition.remove(partition);
            streamingMetrics.processorDestruction.record();
        }
        // clear buffered records
        this.choosyConsumer.clear();
    }
    
    private class KafkaStreamingMetrics {
        final Sensor commitTime;
        final Sensor processTime;
        final Sensor windowTime;
        final Sensor processorCreation;
        final Sensor processorDestruction;
        
        public KafkaStreamingMetrics() {
            String group = "kafka-streaming";
            
            this.commitTime = metrics.sensor("commit-time");
            this.commitTime.add(new MetricName(group, "commit-time-avg-ms"), new Avg());
            this.commitTime.add(new MetricName(group, "commits-time-max-ms"), new Max());
            this.commitTime.add(new MetricName(group, "commits-per-second"), new Rate(new Count()));
            
            this.processTime = metrics.sensor("process-time");
            this.commitTime.add(new MetricName(group, "process-time-avg-ms"), new Avg());
            this.commitTime.add(new MetricName(group, "process-time-max-ms"), new Max());
            this.commitTime.add(new MetricName(group, "process-calls-per-second"), new Rate(new Count()));
            
            this.windowTime = metrics.sensor("window-time");
            this.windowTime.add(new MetricName(group, "window-time-avg-ms"), new Avg());
            this.windowTime.add(new MetricName(group, "window-time-max-ms"), new Max());
            this.windowTime.add(new MetricName(group, "window-calls-per-second"), new Rate(new Count()));
            
            this.processorCreation = metrics.sensor("processor-creation");
            this.processorCreation.add(new MetricName(group, "processor-creation"), new Rate(new Count()));
            
            this.processorDestruction = metrics.sensor("processor-destruction");
            this.processorDestruction.add(new MetricName(group, "processor-destruction"), new Rate(new Count()));
            
        }
        
    }

}
