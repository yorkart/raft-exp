package me.yorkart.raft.exp.core.storage;

import java.io.IOException;
import java.util.List;

/**
 * @author wangyue1
 * @date 2019/2/14
 */
public abstract class Storage {

    protected String path;

    public Storage(String path) {
        this.path = path;
    }

    public abstract void mkdirs();

    public abstract Storage rename(String newPath);

    public abstract void open(String mode) throws IOException;

    public abstract void close() throws IOException;

    public abstract int read(byte b[]) throws IOException;

    public abstract byte[] readAll() throws IOException;

    public abstract void write(byte b[]) throws IOException;

    public abstract long getFilePointer() throws IOException;

    public abstract void seek(long pos) throws IOException;

    public abstract long length() throws IOException;

    public String getPath()  {
        return path;
    }

    public abstract List<String> getSortedFilesInDir();
}
