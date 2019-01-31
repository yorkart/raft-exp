package me.yorkart.raft.exp.core.rpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import me.yorkart.raft.exp.core.proto.RaftConsensusServiceGrpc;
import me.yorkart.raft.exp.core.proto.RaftMessage;

import java.util.concurrent.TimeUnit;

/**
 * @author wangyue1
 * @date 2019/1/30
 */
public class RpcClientImpl implements RpcClient{

    private String ip;
    private int port;

    private ManagedChannel managedChannel;
    private RaftConsensusServiceGrpc.RaftConsensusServiceBlockingStub consensusServiceBlockingStub;

    public RpcClientImpl(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public void start() {
        managedChannel = ManagedChannelBuilder
                .forAddress(ip, port)
                .idleTimeout(2, TimeUnit.SECONDS)
                .usePlaintext()
                .build();

        consensusServiceBlockingStub = RaftConsensusServiceGrpc.newBlockingStub(managedChannel);
    }

    public void stop() {
        if (managedChannel != null) {
            managedChannel.shutdown();
        }
    }

    @Override
    public RaftMessage.VoteResponse preVote(RaftMessage.VoteRequest request) {
        return consensusServiceBlockingStub.preVote(request);
    }

    @Override
    public RaftMessage.VoteResponse requestVote(RaftMessage.VoteRequest request) {
        return consensusServiceBlockingStub.requestVote(request);
    }
}
