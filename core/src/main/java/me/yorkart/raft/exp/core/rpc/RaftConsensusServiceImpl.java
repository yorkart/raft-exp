package me.yorkart.raft.exp.core.rpc;

import io.grpc.stub.StreamObserver;
import me.yorkart.raft.exp.core.proto.RaftConsensusServiceGrpc;
import me.yorkart.raft.exp.core.proto.RaftMessage;

/**
 * @author wangyue1
 * @date 2019/1/30
 */
public class RaftConsensusServiceImpl extends RaftConsensusServiceGrpc.RaftConsensusServiceImplBase {

    @Override
    public void preVote(RaftMessage.VoteRequest request, StreamObserver<RaftMessage.VoteResponse> responseObserver) {
        RaftMessage.VoteResponse response = RaftMessage.VoteResponse.newBuilder()
                .setTerm(1)
                .setVoteGranted(true)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void requestVote(RaftMessage.VoteRequest request, StreamObserver<RaftMessage.VoteResponse> responseObserver) {
        super.requestVote(request, responseObserver);
    }

    @Override
    public void appendEntries(RaftMessage.AppendEntriesRequest request, StreamObserver<RaftMessage.AppendEntriesResponse> responseObserver) {
        super.appendEntries(request, responseObserver);
    }

    @Override
    public void installSnapshot(RaftMessage.InstallSnapshotRequest request, StreamObserver<RaftMessage.InstallSnapshotResponse> responseObserver) {
        super.installSnapshot(request, responseObserver);
    }

}
