package me.yorkart.raft.exp.core.storage;

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
     * path:
     * /xxx/${logDir}/log/
     * /xxx/${logDir}/log/metadata
     * /xxx/${logDir}/log/data/
     * /xxx/${logDir}/log/data/segment-open-{startIndex}
     * /xxx/${logDir}/log/data/segment-{startIndex}-{endIndex}
     */

    private final String logDir;
    private final String logDataDir;
    private final String logMetadataFilePath;

    private final long maxSegmentSize;
    private AtomicLong totalSize = new AtomicLong();

    private RaftMessage.LogMetaData metaData;
    private TreeMap<Long, Segment> startLogIndexSegmentMap = new TreeMap<>();

    public Log(String dataDir, long maxSegmentSize) {
        this.logDir = dataDir + File.separator + "log";
        this.logDataDir = logDir + File.separator + "data";
        this.logMetadataFilePath = logDir + File.separator + "metadata";

        new StorageMemory(logDataDir).mkdirs();

        this.maxSegmentSize = maxSegmentSize;
        this.metaData = readMetadata();
        if (this.metaData == null) {
            if (startLogIndexSegmentMap.size() > 0) {
                throw new RuntimeException("No readable metadata file but found segments in " + logDir);
            }
            this.metaData = RaftMessage.LogMetaData.newBuilder().setFirstLogIndex(1).build();
        }
        logger.info("metadata: " + metaData.toString());
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
        return metaData.getFirstLogIndex();
    }

    public long getEntryTerm(long index) {
        return 0L;
    }

    public void updateMetaData(Long currentTerm, Integer votedFor, Long firstLogIndex) {
        RaftMessage.LogMetaData.Builder builder = RaftMessage.LogMetaData.newBuilder(this.metaData);
        if (currentTerm != null) {
            builder.setCurrentTerm(currentTerm);
        }

        if (votedFor != null) {
            builder.setVotedFor(votedFor);
        }

        if (firstLogIndex != null) {
            builder.setFirstLogIndex(firstLogIndex);
        }

        this.metaData = builder.build();

        try {
            Storage storage = new StorageMemory(logMetadataFilePath);
            storage.open("rw");
            storage.write(this.metaData.toByteArray());

            logger.info("new segment meta info, currentTerm={}, votedFor={}, firstLogIndex={}",
                    metaData.getCurrentTerm(), metaData.getVotedFor(), metaData.getFirstLogIndex());
        } catch (IOException e) {
            logger.warn("meta file not exist, name={}", logMetadataFilePath);
        }
    }

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
        String newSegmentFileName = String.format("open-%d", startIndex);
        String fileName = logDataDir + File.separator + newSegmentFileName;

        Segment segment = new Segment(fileName);
        segment.setCanWrite(true);
        segment.setStartIndex(startIndex);
        segment.setEndIndex(0);

        startLogIndexSegmentMap.put(startIndex, segment);
        return segment;
    }

    private Segment renameSegment(Segment segment) throws IOException {
        String newFileName = String.format("%020d-%020d", segment.getStartIndex(), segment.getEndIndex());

        segment.setCanWrite(false);
        segment.rename(newFileName);

        startLogIndexSegmentMap.put(segment.getStartIndex(), segment);
        return segment;
    }

    public long append(List<RaftMessage.LogEntry> entries) {
        long newLastLogIndex = this.getLastLogIndex();
        for (RaftMessage.LogEntry entry : entries) {
            newLastLogIndex++;

            int entrySize = entry.getSerializedSize(); // 对象序列化后的大小
            int segmentSize = startLogIndexSegmentMap.size();

            Segment latestSegment;
            try {
                if (segmentSize == 0) {
                    latestSegment = createSegment(newLastLogIndex);
                } else {
                    Segment segment = startLogIndexSegmentMap.lastEntry().getValue();
                    if (!segment.isCanWrite()) {
                        latestSegment = createSegment(newLastLogIndex);
                    } else if (segment.getSize() + entrySize >= maxSegmentSize) {
                        latestSegment = renameSegment(segment);
                    } else {
                        latestSegment = segment;
                    }
                }

                if (entry.getIndex() == 0) {
                    entry = RaftMessage.LogEntry.newBuilder()
                            .setIndex(newLastLogIndex)
                            .build();
                }

                latestSegment.setEndIndex(entry.getIndex());
                latestSegment.getEntries().add(new Record(0L, entry));

                totalSize.addAndGet(entrySize);
            } catch (Exception e) {
                throw new RuntimeException("append raft log exception", e);
            }
        }

        return newLastLogIndex;
    }

    /**
     * 删除索引之后的日志
     * @param index 索引，包含该索引
     */
    public void deleteAfterIndex(long index) {

    }

    /**
     * 删除索引之前的日志
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
    void readSegment() {
        List<String> fileNames = new StorageMemory(logDataDir).getSortedFilesInDir();

        try {
            for (String fileName : fileNames) {
                if (!fileName.startsWith("segment-")) {
                    logger.warn("segment filename[{}] is not valid", fileName);
                    continue;
                }

                String[] splitArray = fileName.split("-");
                if (splitArray.length != 3) {
                    logger.warn("segment filename[{}] is not valid", fileName);
                    continue;
                }

                Segment segment = new Segment(fileName);

                try {
                    if (splitArray[1].equals("open")) {
                        segment.setCanWrite(true);
                        segment.setStartIndex(Long.valueOf(splitArray[2]));
                        segment.setEndIndex(0);
                    } else {
                        segment.setCanWrite(false);
                        segment.setStartIndex(Long.valueOf(splitArray[1]));
                        segment.setEndIndex(Long.valueOf(splitArray[2]));
                    }
                } catch (NumberFormatException e) {
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

    public RaftMessage.LogMetaData readMetadata() {
        try {
            Storage storage = new StorageMemory(logMetadataFilePath);
            storage.open("r");
            byte[] data = storage.readAll();

            return RaftMessage.LogMetaData.parseFrom(data);
        } catch (Exception e) {
            logger.warn("meta file not exist, name={}", logMetadataFilePath);
            return null;
        }
    }

    private void updateMetadata(Long currentTerm, Integer votedFor, Long firstLogIndex) {
        RaftMessage.LogMetaData.Builder builder = RaftMessage.LogMetaData.newBuilder(this.metaData);
        if (currentTerm != null) {
            builder.setCurrentTerm(currentTerm);
        }
        if (votedFor != null) {
            builder.setVotedFor(votedFor);
        }
        if (firstLogIndex != null) {
            builder.setFirstLogIndex(firstLogIndex);
        }
        this.metaData = builder.build();

        try {
            Storage storage = new StorageMemory(logMetadataFilePath);
            storage.seek(0);
            storage.write(this.metaData.toByteArray());
            storage.close();
        } catch (IOException e) {
            logger.warn("meta file not exist, name={}", logMetadataFilePath);
        }
    }
}
