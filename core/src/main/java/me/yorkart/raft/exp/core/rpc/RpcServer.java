package me.yorkart.raft.exp.core.rpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

/**
 * @author wangyue1
 * @date 2019/1/30
 */
public class RpcServer {

    private int port ;

    private Server server;

    public RpcServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new RaftConsensusServiceImpl())
                .build()
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread(RpcServer.this::stop));
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
            System.err.println("*** server shut down");
        }
    }
}
