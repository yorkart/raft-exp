package me.yorkart.raft.exp.core.storage;

import com.google.protobuf.InvalidProtocolBufferException;
import me.yorkart.raft.exp.core.proto.RaftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class Log {
    private static Logger logger = LoggerFactory.getLogger(Log.class);

    /**
     * log path layout:
     * ${logDir}/log/
     * ${logDir}/log/metadata
     * ${logDir}/log/data/segment-{in_progress|closed}
     */

    private final String logDir;
    private final String logDataDir;

    private final long maxSegmentSize;
    private AtomicLong totalSize = new AtomicLong();

    private Metadata metadata;
    private TreeMap<Long, Segment> startLogIndexSegmentMap = new TreeMap<>();

    public Log(String dataDir, long maxSegmentSize) {
        this.maxSegmentSize = maxSegmentSize;

        this.logDir = dataDir + File.separator + "log";
        this.logDataDir = logDir + File.separator + "data";

        new StorageFS(logDataDir).mkdirs();

        this.metadata = new Metadata(logDir + File.separator + "metadata");
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public long getLastLogIndex() {
        // 有两种情况segment为空
        // 1、第一次初始化，firstLogIndex = 1，lastLogIndex = 0
        // 2、snapshot刚完成，日志正好被清理掉，firstLogIndex = snapshotIndex + 1， lastLogIndex = snapshotIndex
        if (startLogIndexSegmentMap.size() == 0) {
            return getFirstLogIndex() - 1;
        }

        Segment lastSegment = startLogIndexSegmentMap.lastEntry().getValue();
        return lastSegment.getEndIndex();
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

//    /**
//     * 更新元数据
//     *
//     * @param currentTerm
//     * @param votedFor
//     * @param firstLogIndex
//     */
//    public void save(Long currentTerm, Integer votedFor, Long firstLogIndex) {
//        RaftMessage.LogMetaData.Builder builder = RaftMessage.LogMetaData.newBuilder(this.metaData);
//        if (currentTerm != null) {
//            builder.setCurrentTerm(currentTerm);
//        }
//
//        if (votedFor != null) {
//            builder.setVotedFor(votedFor);
//        }
//
//        if (firstLogIndex != null) {
//            builder.setFirstLogIndex(firstLogIndex);
//        }
//
//        this.metaData = builder.build();
//
//        try {
//            Storage storage = StorageFS.openRW(logMetadataFilePath);
//            storage.write(this.metaData.toByteArray());
//
//            logger.info("new segment meta info, currentTerm={}, votedFor={}, firstLogIndex={}",
//                    metaData.getCurrentTerm(), metaData.getVotedFor(), metaData.getFirstLogIndex());
//        } catch (IOException e) {
//            logger.warn("meta file not exist, name={}", logMetadataFilePath);
//        }
//    }

    public RaftMessage.LogEntry getEntry(long index) {
        long firstLogIndex = getFirstLogIndex();
        long lastLogIndex = getLastLogIndex();

        if (index == 0 || index < firstLogIndex || index > lastLogIndex) {
            logger.debug("index out of range, index={}, firstLogIndex={}, lastLogIndex={}", index, firstLogIndex, lastLogIndex);
            return null;
        }

        if (startLogIndexSegmentMap.size() == 0) {
            return null;
        }

        Segment segment = startLogIndexSegmentMap.floorEntry(index).getValue();
        return segment.getEntry(index);
    }

    private Segment createSegment(long startIndex) throws IOException {
        Segment segment = Segment.create(logDataDir, startIndex);
        startLogIndexSegmentMap.put(startIndex, segment);
        return segment;
    }

    private Segment getLatestSegment(long newLastLogIndex, int appendEntrySize) throws IOException {
        int segmentSize = startLogIndexSegmentMap.size();

        if (segmentSize == 0) {
            return createSegment(newLastLogIndex);
        }

        Segment latestSegment = startLogIndexSegmentMap.lastEntry().getValue();
        if (!latestSegment.isCanWrite()) {
            return createSegment(newLastLogIndex);
        }

        if (latestSegment.getSize() + appendEntrySize >= maxSegmentSize) {
            latestSegment.close();
            return createSegment(newLastLogIndex);
        }

        return latestSegment;
    }


    public long append(List<RaftMessage.LogEntry> entries) {
        long newLastLogIndex = this.getLastLogIndex();
        for (RaftMessage.LogEntry entry : entries) {
            newLastLogIndex++;

//            int entrySize = entry.getSerializedSize(); // 对象序列化后的大小
            byte[] entryBytes = entry.toByteArray();
            int entrySize = entryBytes.length;

            try {
                Segment latestSegment = getLatestSegment(newLastLogIndex, entrySize);

                // TODO 是否支持0，重新构造data数据怎么处理？
//                if (entry.getIndex() == 0) {
//                    entry = RaftMessage.LogEntry.newBuilder()
//                            .setIndex(newLastLogIndex)
//                            .build();
//                }

                latestSegment.append(entry, entryBytes);

                totalSize.addAndGet(entrySize);
            } catch (Exception e) {
                throw new RuntimeException("append raft log exception", e);
            }
        }

        return newLastLogIndex;
    }

    /**
     * 删除索引之后的日志
     * leader变更，数据不一致时需要删除为commit日志
     *
     * @param index 索引，包含该索引
     */
    public void deleteAfterIndex(long index) {
        if (index >= getLastLogIndex()) {
            return;
        }

        logger.info("Truncating log from old end index {} to new end index {}", getLastLogIndex(), index);

        while (!startLogIndexSegmentMap.isEmpty()) {
            Segment segment = startLogIndexSegmentMap.lastEntry().getValue();
            if (index == segment.getEndIndex()) {
                break;
            }

            try {
                if (index < segment.getStartIndex()) {
                    totalSize.addAndGet(-1 * segment.getSize());
                    segment.remove();
                } else if (index < segment.getEndIndex()) {
                    long size = segment.getSize();
                    segment.deleteAfterIndex(index);
                    long newSize = segment.getSize();
                    segment.close();

                    totalSize.addAndGet(newSize - size);
                }
            } catch (IOException e) {
                logger.warn("io exception", e);
            }
        }
    }

    /**
     * 删除索引之前的日志
     *
     * @param index
     */
    public void deleteBeforeIndex(long index) {

    }

    public long getTotalSize() {
        return totalSize.get();
    }

    /**
     * load segment from storage
     */
    void listSegments() {
        List<String> fileNames = new StorageFS(logDataDir).getSortedFilesInDir();

        try {
            for (String fileName : fileNames) {
                Segment segment;
                try {
                    segment = Segment.load(logDataDir, fileName);
                } catch (NumberFormatException e) {
                    logger.warn("segment filename[{}] is not valid", fileName);
                    continue;
                }

                if (segment == null) {
                    logger.warn("segment filename[{}] is not valid", fileName);
                    continue;
                }

                startLogIndexSegmentMap.put(segment.getStartIndex(), segment);
            }
        } catch (IOException e) {
            logger.warn("readSegments exception:", e);
            throw new RuntimeException("open segment file error");
        }
    }

    public void close() {
        for (Segment segment : startLogIndexSegmentMap.values()) {
            try {
                segment.close();
            } catch (IOException e) {
                logger.error("close segment error, path: " + segment, e);
            }
        }
    }
}
