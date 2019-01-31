package me.yorkart.raft.exp.example;

import me.yorkart.raft.exp.core.rpc.RpcServer;

import java.io.IOException;

/**
 * @author wangyue1
 * @date 2019/1/31
 */
public class RaftServerExample {

    public static void main(String[] args) throws IOException, InterruptedException {

        RpcServer rpcServer = new RpcServer(8688);
        try {
            rpcServer.start();

            Thread.currentThread().join();
        } finally {
            rpcServer.stop();
        }

    }
}
