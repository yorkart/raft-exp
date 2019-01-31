package me.yorkart.raft.exp.core;

import me.yorkart.raft.exp.core.proto.RaftMessage;
import me.yorkart.raft.exp.core.rpc.RpcClientImpl;

/**
 * @author wangyue1
 * @date 2019/1/30
 */
public class RaftApplication {

    public static void main(String[] args) throws InterruptedException {
        RpcClientImpl rpcClient = new RpcClientImpl("127.0.0.1", 8688);
        rpcClient.start();

        Thread.sleep(1000);

        while(true) {
            try {
                RaftMessage.VoteRequest request = RaftMessage.VoteRequest.newBuilder()
                        .setCandidateId(1)
                        .setTerm(0)
                        .setLastLogIndex(10)
                        .setLastLogTerm(3)
                        .build();
                RaftMessage.VoteResponse response = rpcClient.preVote(request);
                System.out.println(response.toString());
            }catch (Exception e) {
                e.printStackTrace();
            }

            Thread.sleep(1000);
        }

//        rpcClient.stop();
    }

}
