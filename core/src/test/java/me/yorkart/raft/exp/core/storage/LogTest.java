package me.yorkart.raft.exp.core.storage;

import com.google.protobuf.ByteString;
import me.yorkart.raft.exp.core.proto.RaftMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wangyue1
 * @date 2019/2/14
 */
public class LogTest {

    private Log log;

    @Before
    public void logConstruction() {
        log = new Log("/data/raft/log", 20);
    }

    @After
    public void logRelease() throws InterruptedException {
        log.close();
        Thread.sleep(1000);
    }

    @Test
    public void testLogIndex() {
        System.out.println(log.getFirstLogIndex());
        System.out.println(log.getLastLogIndex());
    }

    @Test
    public void testSaveMetadata() {
        log.getMetadata().save(null, null, 2L);
    }

    @Test
    public void testReadMetadata() {
        System.out.println(log.getMetadata().load());
    }

    @Test
    public void testAppend() {
        List<RaftMessage.LogEntry> entries = new ArrayList<>();
        {
            RaftMessage.LogEntry entry = RaftMessage.LogEntry.newBuilder()
                    .setTerm(1)
                    .setIndex(1)
                    .setType(RaftMessage.EntryType.ENTRY_TYPE_DATA)
                    .setData(ByteString.copyFromUtf8("abc1"))
                    .build();
            entries.add(entry);
        }
        {
            RaftMessage.LogEntry entry = RaftMessage.LogEntry.newBuilder()
                    .setTerm(1)
                    .setIndex(2)
                    .setType(RaftMessage.EntryType.ENTRY_TYPE_DATA)
                    .setData(ByteString.copyFromUtf8("abc2"))
                    .build();
            entries.add(entry);
        }
        {
            RaftMessage.LogEntry entry = RaftMessage.LogEntry.newBuilder()
                    .setTerm(1)
                    .setIndex(3)
                    .setType(RaftMessage.EntryType.ENTRY_TYPE_DATA)
                    .setData(ByteString.copyFromUtf8("abc3"))
                    .build();
            entries.add(entry);
        }
        {
            RaftMessage.LogEntry entry = RaftMessage.LogEntry.newBuilder()
                    .setTerm(1)
                    .setIndex(4)
                    .setType(RaftMessage.EntryType.ENTRY_TYPE_DATA)
                    .setData(ByteString.copyFromUtf8("abc4"))
                    .build();
            entries.add(entry);
        }

        log.append(entries);
    }
}
