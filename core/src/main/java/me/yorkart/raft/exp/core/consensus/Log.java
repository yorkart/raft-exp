package me.yorkart.raft.exp.core.consensus;

import me.yorkart.raft.exp.core.proto.RaftMessage;

public class Log {

    public long getLastLogIndex() {
        return 0L;
    }

    public long getFirstLogIndex() {
        return 0L;
    }

    public long getEntryTerm(long index) {
        return 0L;
    }

    public void updateMetaData(Long currentTerm, Integer votedFor, Long firstLogIndex) {

    }

    public RaftMessage.LogMetaData readMetadata() {
        return RaftMessage.LogMetaData.newBuilder().build();
    }
}
