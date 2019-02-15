package me.yorkart.raft.exp.core.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author wangyue1
 * @date 2019/2/14
 */
public class StorageMemory extends Storage {
    private static Logger logger = LoggerFactory.getLogger(StorageMemory.class);

    private RandomAccessFile randomAccessFile;

    public StorageMemory(String path) {
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

        return new StorageMemory(newPath);
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
        return 0;
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
}
