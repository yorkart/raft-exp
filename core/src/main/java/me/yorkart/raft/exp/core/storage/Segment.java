package me.yorkart.raft.exp.core.storage;

import me.yorkart.raft.exp.core.proto.RaftMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wangyue1
 * @date 2019/1/31
 */
public class Segment {

    private boolean canWrite;
    private long startIndex;
    private long endIndex;
    private long size;

    private Storage storage;

    private List<Record> entries = new ArrayList<>();

    public Segment(String fileName) throws IOException {
        this.storage = new StorageMemory(fileName);
        this.storage.open("rw");
        this.size = this.storage.length();
    }

    public void rename(String newFileName) throws IOException {
        storage.close();
        storage = storage.rename(newFileName);
        storage.open("rw");
        this.size = this.storage.length();
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
    public void close() throws IOException {
        storage.close();
    }

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
