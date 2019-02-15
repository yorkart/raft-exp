package me.yorkart.raft.exp.core.storage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author wangyue1
 * @date 2019/2/14
 */
public class LogTest {

    private Log log;

    @Before
    public void logConstruction() {
        log = new Log("/data/raft", 1024*1024* 10);
    }

    @After
    public void logRelease() throws InterruptedException {
        Thread.sleep(1000);
    }

    @Test
    public void testLogIndex() {
        System.out.println(log.getFirstLogIndex());
        System.out.println(log.getLastLogIndex());
    }

    @Test
    public void testUpdateMetadata() {
        log.updateMetaData(null, null, 2L);
    }

    @Test
    public void testReadSegment() {
        log.readSegment();
    }
}
