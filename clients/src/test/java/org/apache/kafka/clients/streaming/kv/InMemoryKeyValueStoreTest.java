package org.apache.kafka.clients.streaming.kv;

import java.util.Properties;

import org.apache.kafka.clients.streaming.ProcessorContext;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Before;

public class InMemoryKeyValueStoreTest extends AbstractKeyValueStoreTest {
    
    private InMemoryKeyValueStore<String, String> store;
    private ProcessorContext context;
    
    public InMemoryKeyValueStoreTest() {
        super(10000);
    }
    
    @Before
    public void setup() {
        this.context = processorContext(new Properties());
        this.store = new InMemoryKeyValueStore<String, String>("test", context);
    }
    
    @After
    public void teardown() {
        if(context != null)
            Utils.rm(context.stateDir());
    }

    @Override
    public KeyValueStore<String, String> store() {
        return store;
    }

}
