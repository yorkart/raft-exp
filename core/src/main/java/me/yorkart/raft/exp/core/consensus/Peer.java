package me.yorkart.raft.exp.core.consensus;

import me.yorkart.raft.exp.core.proto.RaftMessage;
import me.yorkart.raft.exp.core.rpc.RpcClient;
import me.yorkart.raft.exp.core.rpc.RpcClientImpl;

public class Peer {
    private RaftMessage.Server server;

    /**
     * Volatile state on leaders
     */
    // 对于每个server，需要发送到server的下一个日志条目的索引（初始化为leader最后一个日志索引+1）
    // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    private long nextIndex;
    // 对于每个server，server上已经复制最高的日志条目索引值（初始0，持续递增）
    // for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)
    private long matchIndex;

    // 投票是否被准许
    private volatile Boolean voteGranted;

    private RpcClientImpl rpcClient;

    public Peer(RaftMessage.Server server) {
        this.server = server;

        RaftMessage.EndPoint endPoint = this.server.getEndPoint();
        rpcClient = new RpcClientImpl(endPoint.getHost(), endPoint.getPort());
        rpcClient.start();
    }

    public RaftMessage.Server getServer() {
        return server;
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    public Boolean getVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(Boolean voteGranted) {
        this.voteGranted = voteGranted;
    }

    public RpcClient getRpcClient() {
        return rpcClient;
    }
}
