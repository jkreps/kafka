package org.apache.kafka.common.utils;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OffsetCheckpointTest {
    
    private File file;
    OffsetCheckpoint cp;

    @Before
    public void setup() throws Exception {
        this.file = File.createTempFile("temp", ".txt");
        this.file.delete();
        this.cp = new OffsetCheckpoint(file);
    }
    
    @After
    public void teardown() throws Exception {
        this.cp.delete();
    }
    
    @Test
    public void testEmpty() throws Exception {
        assertEquals(Collections.<TopicPartition, Long>emptyMap(), this.cp.read());
    }
    
    @Test
    public void testStoreAndRetrieve() throws Exception {
        Map<TopicPartition, Long> m = new HashMap<TopicPartition, Long>();
        m.put(new TopicPartition("t", 1), 1L);
        m.put(new TopicPartition("t", 2), 2L);
        m.put(new TopicPartition("u", 3), 3L);
        this.cp.write(m);
        assertEquals(m, this.cp.read());
    }
    
}
