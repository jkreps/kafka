package org.apache.kafka.clients.streaming.internals;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.streaming.RecordCollector;
import org.apache.kafka.clients.streaming.StorageEngine;
import org.apache.kafka.common.Entry;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;

public class MockStorageEngine implements StorageEngine {
    
    private final Time time;
    private final List<Entry<byte[], byte[]>> entries;
    private long flushed = 0;
    private long closed = 0;
    
    
    public MockStorageEngine(Time time) {
        this.time = time;
        this.entries = new ArrayList<Entry<byte[], byte[]>>();
    }

    @Override
    public void flush() {
        this.flushed = time.milliseconds();
    }

    @Override
    public void close() {
        this.closed = time.milliseconds();
    }
    
    public long flushed() {
        return this.flushed;
    }
    
    public long closed() {
        return this.closed;
    }
    
    public List<Entry<byte[], byte[]>> puts() {
        return this.entries;
    }

    @Override
    public void registerAndRestore(RecordCollector<byte[], byte[]> collector, 
                                   Consumer<byte[], byte[]> consumer,
                                   TopicPartition partition,
                                   long checkpointedOffset,
                                   long endOffset) {
        throw new IllegalArgumentException("Implement me");
    }

    @Override
    public String name() {
        return "mock";
    }

}
