package me.yorkart.raft.exp.core.storage;

import me.yorkart.raft.exp.core.proto.RaftMessage;

/**
 * @author wangyue1
 * @date 2019/1/31
 */
public class Record {
    long offset;
    RaftMessage.LogEntry entry;

    public Record(long offset, RaftMessage.LogEntry entry) {
        this.offset = offset;
        this.entry = entry;
    }
}
