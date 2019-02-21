package me.yorkart.raft.exp.core.storage;

import me.yorkart.raft.exp.core.proto.RaftMessage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * log path layout:
 * ${logDir}/log/data/segment-in_progress-{startIndex} : open segment
 * ${logDir}/log/data/segment-{startIndex}-{endIndex}  : closed segment
 *
 * @author wangyue1
 * @date 2019/1/31
 */
public class Segment {

    private String path;

    private boolean canWrite;
    private long startIndex;
    private long endIndex;
    private long size;

    private Storage storage;

    // TODO 优化集合为堆外内存，使用固定大小
    private List<Record> entries = new ArrayList<>();

    public static Segment create(String path, long startIndex) throws IOException {
        return openInProgressSegment(path, startIndex);
    }

    public static Segment load(String path, String fileName) throws IOException {
        if (!fileName.startsWith("segment-")) {
            return null;
        }

        String[] splitArray = fileName.split("-");
        if (splitArray.length != 3) {
            throw new RuntimeException("segment filename is not valid: " + fileName);
        }

        if (splitArray[1].equals("in_progress")) {
            return openInProgressSegment(path, Long.valueOf(splitArray[2]));
        } else {
            return openClosedSegment(path, Long.valueOf(splitArray[1]), Long.valueOf(splitArray[2]));
        }
    }

    private static Segment openInProgressSegment(String path, long startIndex) throws IOException {
        Segment segment = new Segment();

        segment.path = path;
        segment.startIndex = startIndex;
        segment.endIndex = 0;

        String newSegmentFileName = String.format("segment-in_progress-%020d", segment.startIndex);
        String fileName = segment.path + File.separator + newSegmentFileName;

        segment.storage = StorageFS.openRW(fileName);
        segment.size = segment.storage.length();
        segment.canWrite = true;

        return segment;
    }

    private static Segment openClosedSegment(String path, long startIndex, long endIndex) throws IOException {
        Segment segment = new Segment();

        segment.path = path;
        segment.startIndex = startIndex;
        segment.endIndex = endIndex;

        String newSegmentFileName = String.format("segment-%020d-%020d", segment.startIndex, segment.endIndex);
        String fileName = segment.path + File.separator + newSegmentFileName;

        segment.storage = StorageFS.openR(fileName);
        segment.size = segment.storage.length();
        segment.canWrite = true;

        return segment;
    }

    private Segment() {
    }

    public void close() throws IOException {
        String newFileName = String.format("segment-%020d-%020d", this.getStartIndex(), this.getEndIndex());
        String newFullFileName = this.path + File.separator + newFileName;

        this.canWrite = false;
        this.rename(newFullFileName);
    }

    public void rename(String newFileName) throws IOException {
        storage.close();
        storage = storage.rename(newFileName);
    }

    public RaftMessage.LogEntry getEntry(long index) {
        if (startIndex == 0 || endIndex == 0) {
            return null;
        }
        if (index < startIndex || index > endIndex) {
            return null;
        }
        int indexInList = (int) (index - startIndex);
        return entries.get(indexInList).entry;
    }

    // segmnet释放工作，因为当前类为内存模拟存储，所有不需要任何操作
//    public void close() throws IOException {
//        storage.close();
//    }

    public boolean isCanWrite() {
        return canWrite;
    }

    public void setCanWrite(boolean canWrite) {
        this.canWrite = canWrite;
    }

    public long getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(long startIndex) {
        this.startIndex = startIndex;
    }

    public long getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(long endIndex) {
        this.endIndex = endIndex;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public List<Record> getEntries() {
        return entries;
    }

    public void setEntries(List<Record> entries) {
        this.entries = entries;
    }

    public String getFileName() {
        return storage.getPath();
    }

    public Storage getStorage() {
        return storage;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }
}
