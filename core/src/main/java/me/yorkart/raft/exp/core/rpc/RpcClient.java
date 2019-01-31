package me.yorkart.raft.exp.core.rpc;

import me.yorkart.raft.exp.core.proto.RaftMessage;

/**
 * @author wangyue1
 * @date 2019/1/31
 */
public interface RpcClient {

    RaftMessage.VoteResponse preVote(RaftMessage.VoteRequest request);

    RaftMessage.VoteResponse requestVote(RaftMessage.VoteRequest request);
}
