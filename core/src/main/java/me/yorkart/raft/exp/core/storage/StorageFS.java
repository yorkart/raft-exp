package me.yorkart.raft.exp.core.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author wangyue1
 * @date 2019/2/14
 */
public class StorageFS extends Storage {
    private static Logger logger = LoggerFactory.getLogger(StorageFS.class);

    private RandomAccessFile randomAccessFile;

    public StorageFS(String path) {
        super(path);
    }

    @Override
    public void mkdirs()  {
        File file = new File(path);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    @Override
    public Storage rename(String newPath) {
        File oldFile = new File(path);
        File newFile = new File(newPath);

        boolean success = oldFile.renameTo(newFile);
        if (!success) {
            throw new RuntimeException("rename error");
        }

        return new StorageFS(newPath);
    }

    @Override
    public void remove() {
        File file = new File(path);
        if (file.exists()){
            file.delete();
        }
    }

    @Override
    public void open(String mode) throws IOException {
        try {
            File file = new File(path);
            this.randomAccessFile = new RandomAccessFile(file, mode);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("file not found, file=" + path);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (randomAccessFile != null) {
                randomAccessFile.close();
            }
        } catch (IOException ex) {
            logger.warn("close file error, msg={}", ex.getMessage());
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        return randomAccessFile.read(b);
    }

    @Override
    public int readInt() throws IOException {
        return randomAccessFile.readInt();
    }

    @Override
    public byte[] readAll() throws IOException {
        randomAccessFile.seek(0);
        int length = (int)randomAccessFile.length();

        byte[] data = new byte[length];
        randomAccessFile.read(data);

        return new byte[0];
    }

    @Override
    public void writeInt(int v) throws IOException {
        randomAccessFile.writeInt(v);
    }

    @Override
    public void write(byte[] b) throws IOException {
        randomAccessFile.write(b);
    }

    @Override
    public long getFilePointer() throws IOException {
        return randomAccessFile.getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
        randomAccessFile.seek(pos);
    }

    @Override
    public long length() throws IOException {
        return randomAccessFile.length();
    }

    @Override
    public void truncate(long size) throws IOException {
        FileChannel fileChannel = randomAccessFile.getChannel();
        fileChannel.truncate(size);
        fileChannel.close();
    }

    @Override
    public List<String> getSortedFilesInDir() {
        File dir = new File(path);
        File[] children = dir.listFiles();
        if (children == null) {
            return Collections.emptyList();
        }

        return Arrays.stream(children).map(File::getName).collect(Collectors.toList());
    }

    public static Storage openR(String path) throws IOException {
        Storage storage = new StorageFS(path);
        storage.open("r");
        return storage;
    }

    public static Storage openRW(String path) throws IOException {
        Storage storage = new StorageFS(path);
        storage.open("rw");
        return storage;
    }
}
