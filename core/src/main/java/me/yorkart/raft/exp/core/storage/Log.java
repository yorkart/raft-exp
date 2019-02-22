package me.yorkart.raft.exp.core.storage;

import me.yorkart.raft.exp.core.proto.RaftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * log index start with 1
 * log path layout:
 * ${logDir}/metadata
 * ${logDir}/data/segment-{in_progress|closed}
 */
public class Log {
    private static Logger logger = LoggerFactory.getLogger(Log.class);

    private Metadata metadata;
    private Segments segments;

    public Log(String logDir, long maxSegmentSize) {
        this.metadata = new Metadata(logDir + File.separator + "metadata");
        this.segments = new Segments(logDir + File.separator + "data", maxSegmentSize, metadata);
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public long getLastLogIndex() {
//        // 有两种情况segment为空
//        // 1、第一次初始化，firstLogIndex = 1，lastLogIndex = 0
//        // 2、snapshot刚完成，日志正好被清理掉，firstLogIndex = snapshotIndex + 1， lastLogIndex = snapshotIndex
//        if (startLogIndexSegmentMap.size() == 0) {
//            return getFirstLogIndex() - 1;
//        }
//
//        Segment lastSegment = startLogIndexSegmentMap.lastEntry().getValue();
//        return lastSegment.getEndIndex();

        return segments.getLastLogIndex();
    }

    public long getFirstLogIndex() {
        return metadata.getFirstLogIndex();
    }

    public long getEntryTerm(long index) {
        RaftMessage.LogEntry entry = getEntry(index);
        if (entry == null) {
            return 0;
        }

        return entry.getTerm();
    }

    public RaftMessage.LogEntry getEntry(long index) {
        long firstLogIndex = getFirstLogIndex();
        long lastLogIndex = getLastLogIndex();

        if (index == 0 || index < firstLogIndex || index > lastLogIndex) {
            logger.debug("index out of range, index={}, firstLogIndex={}, lastLogIndex={}", index, firstLogIndex, lastLogIndex);
            return null;
        }

        return segments.getEntry(index);
    }

    public long append(List<RaftMessage.LogEntry> entries) {
        return segments.append(entries);
    }

    /**
     * 删除索引之后的日志
     * leader变更，数据不一致时需要删除为commit日志
     *
     * @param index 索引，包含该索引
     */
    public void truncateSuffix(long index) {
        segments.truncateSuffix(index);
    }

    /**
     * 删除索引之前的日志
     *
     * @param index
     */
    public void deleteBeforeIndex(long index) {

    }

    public void close() {
        segments.closeAll();
    }
}
