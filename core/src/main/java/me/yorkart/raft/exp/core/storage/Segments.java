package me.yorkart.raft.exp.core.storage;

import me.yorkart.raft.exp.core.proto.RaftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wangyue1
 * @date 2019/2/21
 */
public class Segments {
    private static Logger logger = LoggerFactory.getLogger(Segments.class);

    private final TreeMap<Long, Segment> segmentMap = new TreeMap<>();
    private final AtomicLong totalSize = new AtomicLong();
    private final AtomicLong latestLogIndex = new AtomicLong(0);
    private final Metadata metadata;

    private final String logDataDir;
    private final long maxSegmentSize;

    public Segments(String logDataDir, long maxSegmentSize, Metadata metadata) {
        this.logDataDir = logDataDir;
        this.maxSegmentSize = maxSegmentSize;
        this.metadata = metadata;

        new StorageFS(logDataDir).mkdirs();

        listSegments();

        if (latestLogIndex.get() == 0) {
            latestLogIndex.set(getFirstLogIndex() -1);
        }
    }

    public long getFirstLogIndex() {
        return metadata.getFirstLogIndex();
    }

    public long getLastLogIndex() {
        return  latestLogIndex.get();
    }

    private void listSegments() {
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

                segmentMap.put(segment.getStartIndex(), segment);

                if (segment.getEndIndex() > latestLogIndex.get()) {
                    latestLogIndex.set(segment.getEndIndex());
                }
            }
        } catch (IOException e) {
            logger.warn("readSegments exception:", e);
            throw new RuntimeException("open segment file error");
        }
    }

    private Segment createSegment(long startIndex) throws IOException {
        Segment segment = Segment.create(logDataDir, startIndex);
        segmentMap.put(startIndex, segment);
        return segment;
    }

    private Segment getLatestSegment(long newLastLogIndex, int appendEntrySize) throws IOException {
        int segmentSize = segmentMap.size();

        if (segmentSize == 0) {
            return createSegment(newLastLogIndex);
        }

        Segment latestSegment = segmentMap.lastEntry().getValue();
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
        for (RaftMessage.LogEntry entry : entries) {
            long newLastLogIndex = this.latestLogIndex.incrementAndGet();

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

        return this.latestLogIndex.get();
    }

    public RaftMessage.LogEntry getEntry(long index) {
        if (segmentMap.size() == 0) {
            return null;
        }

        Segment segment = segmentMap.floorEntry(index).getValue();
        return segment.getEntry(index);
    }

    /**
     * delete logs from storage's head, [1, first_index_kept) will be discarded
     * @param first_index_kept
     */
    public void truncatePrefix(long first_index_kept) {

    }

    /**
     * 删除索引之后的日志
     * leader变更，数据不一致时需要删除为commit日志
     * delete uncommitted logs from storage's tail, (last_index_kept, infinity) will be discarded
     *
     * @param lastIndexKept 索引，包含该索引
     */
    public void truncateSuffix(long lastIndexKept) {
        if (lastIndexKept >= latestLogIndex.get()) {
            return;
        }

        logger.info("Truncating log from old end index {} to new end index {}", latestLogIndex.get(), lastIndexKept);

        while (!segmentMap.isEmpty()) {
            Segment segment = segmentMap.lastEntry().getValue();
            if (lastIndexKept == segment.getEndIndex()) {
                break;
            }

            try {
                if (lastIndexKept < segment.getStartIndex()) {
                    totalSize.addAndGet(-1 * segment.getSize());
                    segment.remove();
                } else if (lastIndexKept < segment.getEndIndex()) {
                    long size = segment.getSize();
                    segment.truncateSuffix(lastIndexKept);
                    long newSize = segment.getSize();
                    segment.close();

                    totalSize.addAndGet(newSize - size);
                }
            } catch (IOException e) {
                logger.warn("io exception", e);
            }
        }

        latestLogIndex.set(lastIndexKept);
    }

    public void closeAll() {
        for (Segment segment : segmentMap.values()) {
            try {
                segment.close();
            } catch (IOException e) {
                logger.error("close segment error, path: " + segment, e);
            }
        }
    }
}
