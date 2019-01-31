package me.yorkart.raft.exp.core.consensus;

import me.yorkart.raft.exp.core.proto.RaftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;

/**
 * @author wangyue1
 * @date 2019/1/31
 */
public class Peers {
    private static Logger logger = LoggerFactory.getLogger(Peers.class);

    private final RaftMessage.Server localServer;
    private final ConcurrentMap<Integer, Peer> peerMap = new ConcurrentHashMap<>();

    private ScheduledFuture heartbeatScheduledFuture;

    public Peers(RaftMessage.Server localServer,
                 RaftMessage.Configuration configuration,
                 long raftLogLastIndex) {
        this.localServer = localServer;

        for (RaftMessage.Server server : configuration.getServersList()) {
            if (!peerMap.containsKey(server.getId()) && server.getId() != localServer.getId()) {
                Peer peer = new Peer(server);
                peer.setNextIndex(raftLogLastIndex);
                peerMap.put(server.getId(), peer);
            }
        }
    }

    public int size() {
        return peerMap.size();
    }

    public void forEach(Consumer<Peer> action) {
        peerMap.values().forEach(action);
    }

    public Collection<Peer> getPeers() {
        return peerMap.values();
    }

    public int getVoteGrantedNum() {
        return (int) peerMap.values().stream()
                .filter(x -> x.getVoteGranted() != null && x.getVoteGranted())
                .count();
    }

    void startHeartbeat() {
        logger.debug("start new heartbeat, peers={}", peerMap.keySet());
        for (final Peer peer : peerMap.values()) {
//            executorService.submit(new Runnable() {
//                @Override
//                public void run() {
//                    // todo
////                    appendEntries(peer);
//                }
//            });
        }
//        resetHeartbeatTimer();
    }

    void stopHeartbeat() {

    }
}
