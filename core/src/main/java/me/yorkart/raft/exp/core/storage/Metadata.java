package me.yorkart.raft.exp.core.storage;

import com.google.protobuf.InvalidProtocolBufferException;
import me.yorkart.raft.exp.core.proto.RaftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author wangyue1
 * @date 2019/2/21
 */
public class Metadata {
    private static Logger logger = LoggerFactory.getLogger(Metadata.class);

    private String logMetadataFilePath;
    private volatile RaftMessage.LogMetaData metadata;

    public Metadata(String logMetadataFilePath) {
        this.logMetadataFilePath = logMetadataFilePath;
        this.metadata = load();

        if (this.metadata == null) {
            this.metadata = RaftMessage.LogMetaData.newBuilder().setFirstLogIndex(1).build();
            logger.info("metadata is empty and init: " + metadata.toString());
        } else {
            logger.info("metadata load: " + metadata.toString());
        }
    }

    public long getFirstLogIndex() {
        return metadata.getFirstLogIndex();
    }

    public long getCurrentTerm(){
        return metadata.getCurrentTerm();
    }

    public int getVotedFor() {
        return metadata.getVotedFor();
    }

    public RaftMessage.LogMetaData load() {
        byte[] data;
        int len;
        int readLen;
        try {
            Storage storage = StorageFS.openR(logMetadataFilePath);
            len = storage.readInt();
            data = new byte[len];
            readLen = storage.read(data);
        } catch (Exception e) {
            logger.warn("meta file not exist, name={}", logMetadataFilePath);
            return null;
        }

        if (readLen != len) {
            logger.warn("meta file size discord");
            return null;
        }

        try {
            return RaftMessage.LogMetaData.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            logger.warn("meta parse to proto error", e);
            return null;
        }
    }

    public void save(Long currentTerm, Integer votedFor, Long firstLogIndex) {
        RaftMessage.LogMetaData.Builder builder = RaftMessage.LogMetaData.newBuilder(this.metadata);
        if (currentTerm != null) {
            builder.setCurrentTerm(currentTerm);
        }

        if (votedFor != null) {
            builder.setVotedFor(votedFor);
        }

        if (firstLogIndex != null) {
            builder.setFirstLogIndex(firstLogIndex);
        }

        this.metadata = builder.build();
        byte[] data = this.metadata.toByteArray();
        try {
            Storage storage = StorageFS.openRW(logMetadataFilePath);
            storage.seek(0);
            storage.writeInt(data.length);
            storage.write(data);
            storage.close();

            logger.info("new segment meta info, currentTerm={}, votedFor={}, firstLogIndex={}",
                    metadata.getCurrentTerm(), metadata.getVotedFor(), metadata.getFirstLogIndex());
        } catch (IOException e) {
            logger.warn("meta file not exist, name={}", logMetadataFilePath);
        }
    }
}
