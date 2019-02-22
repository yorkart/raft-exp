package me.yorkart.raft.exp.core.consensus;

import me.yorkart.raft.exp.core.proto.RaftMessage;
import me.yorkart.raft.exp.core.storage.Log;
import me.yorkart.raft.exp.core.storage.SequenceStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LeaderElection {
    private static Logger logger = LoggerFactory.getLogger(LeaderElection.class);

    private RaftOptions raftOptions;

    // 选举任务线程池
    private ExecutorService executorService;
    //
    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture electionScheduledFuture;

    private ScheduledFuture heartbeatScheduledFuture;

    /**
     * Persistent state on all servers
     */
    // 服务器最后一次知道的任期号（初始化为 0，持续递增）
    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    private volatile long currentTerm;
    // 在当前获得选票的候选人的Id
    // candidateId that received vote in current term (or null if none)
    private volatile int votedFor;
    // 日志条目；每个日志条目包含状态机的命令，以及leader接收日志条目是的term
    // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
    private Log log;

    /**
     * Volatile state on all servers
     */
    // 已知提交的最高日志条目索引（初始0，持续递增）
    // index of highest log entry known to be committed (initialized to 0, increases monotonically)
    private long commitIndex;
    // 应用于状态机的最高日志条目索引（初始0，持续递增）
    // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    private long lastApplied;

    private StateMachine stateMachine;

    private int leaderId; // leader节点id
    private NodeState state = NodeState.FOLLOWER;
    private Lock lock = new ReentrantLock();

    private RaftMessage.Server localServer;
    private RaftMessage.Configuration configuration;

//    private ConcurrentMap<Integer, Peer> peerMap = new ConcurrentHashMap<>();

    private final Peers peers;


    public LeaderElection(RaftOptions raftOptions,
                          RaftMessage.Server localServer,
                          RaftMessage.Configuration configuration,
                          long raftLogLastIndex) {
        this.raftOptions = raftOptions;
        this.localServer = localServer;
        this.configuration = configuration;

        this.peers = new Peers(localServer, configuration, raftLogLastIndex);

//        for (RaftMessage.Server server : configuration.getServersList()) {
//            if (!peerMap.containsKey(server.getId()) && server.getId() != localServer.getId()) {
//                Peer peer = new Peer(server);
//                peer.setNextIndex(raftLogLastIndex);
//                peerMap.put(server.getId(), peer);
//            }
//        }

        stateMachine = new SequenceStateMachine(raftOptions.getDataDir());

        // init from metadata
        log = new Log("", 1024 * 32);
        currentTerm = log.getMetadata().getCurrentTerm();
        votedFor = log.getMetadata().getVotedFor();

        // init thread pool
        executorService = new ThreadPoolExecutor(
                raftOptions.getRaftConsensusThreadNum(),
                raftOptions.getRaftConsensusThreadNum(),
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
        scheduledExecutorService = Executors.newScheduledThreadPool(2);

        // start election
        resetElectionTimer();
    }

    // heartbeat timer, append entries
    // in lock
//    private void resetHeartbeatTimer() {
//        if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
//            heartbeatScheduledFuture.cancel(true);
//        }
//        heartbeatScheduledFuture = scheduledExecutorService.schedule(new Runnable() {
//            @Override
//            public void run() {
//                startNewHeartbeat();
//            }
//        }, raftOptions.getHeartbeatPeriodMilliseconds(), TimeUnit.MILLISECONDS);
//    }

    // in lock, 开始心跳，对leader有效
//    private void startNewHeartbeat() {
//        logger.debug("start new heartbeat, peers={}", peerMap.keySet());
//        for (final Peer peer : peerMap.values()) {
//            executorService.submit(new Runnable() {
//                @Override
//                public void run() {
//                    // todo
////                    appendEntries(peer);
//                }
//            });
//        }
//        resetHeartbeatTimer();
//    }

    private void resetElectionTimer() {
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(true);
        }

        electionScheduledFuture = scheduledExecutorService.schedule(
                this::startPreVote,
                getElectionTimeoutMs(),
                TimeUnit.MILLISECONDS);
    }

    private int getElectionTimeoutMs() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int randomElectionTimeout = raftOptions.getElectionTimeoutMilliseconds()
                + random.nextInt(0, raftOptions.getElectionTimeoutMilliseconds());
        logger.debug("new election time is after {} ms", randomElectionTimeout);
        return randomElectionTimeout;
    }

    private void startPreVote() {
        lock.lock();
        try {
            logger.info("Running pre-vote in term {}", currentTerm);
            state = NodeState.PRE_CANDIDATE;
        } finally {
            lock.unlock();
        }

//        peerMap.values().forEach(peer -> executorService.submit(() -> preVote(peer)));
        peers.forEach(peer -> executorService.submit(() -> preVote(peer)));

        resetElectionTimer();
    }

    private void startVote() {
        lock.lock();
        try {
            currentTerm++;
            logger.info("Running for election in term {}", currentTerm);

            state = NodeState.CANDIDATE;
            leaderId = 0;
            votedFor = localServer.getId();
        } finally {
            lock.unlock();
        }

//        peerMap.values().forEach(peer -> executorService.submit(() -> requestVote(peer)));
        peers.forEach(peer -> executorService.submit(() -> requestVote(peer)));
    }

    private void preVote(Peer peer) {
        logger.info("begin pre vote request");

        RaftMessage.VoteRequest.Builder requestBuilder = RaftMessage.VoteRequest.newBuilder();
        lock.lock();
        try {
            peer.setVoteGranted(null);
            requestBuilder.setCandidateId(localServer.getId())
                    .setTerm(currentTerm)
                    .setLastLogIndex(log.getLastLogIndex())
                    .setLastLogTerm(getLastLogTerm());
        } finally {
            lock.unlock();
        }

        RaftMessage.VoteRequest request = requestBuilder.build();
        RaftMessage.VoteResponse response;
        try {
            response = peer.getRpcClient().preVote(request);
        } catch (Exception e) {
            logger.error("pre vote with peer[" + peer.getServer().getEndPoint().toString() + "] failed", e);
            peer.setVoteGranted(false);
            return;
        }

        lock.lock();
        try {
            boolean voteGranted = response.getVoteGranted();
            long responseTerm = response.getTerm();

            peer.setVoteGranted(voteGranted);

            // 任期不匹配或者当前已经不是候选人状态， 忽略此次选举结果
            if (currentTerm != request.getTerm() || state != NodeState.CANDIDATE) {
                logger.info("ignore requestVote RPC result");
                return;
            }

            // 如果任期大于当前任务，卸任为follower
            if (responseTerm > currentTerm) {
                logger.info("Received RequestVote response from server {} in term {} (this server's term was {})",
                        peer.getServer().getId(), responseTerm, currentTerm);
                stepDown(responseTerm);

                return;
            }

            // 如果被投票
            if (voteGranted) {
                logger.info("Got vote from server {} for term {}", peer.getServer().getId(), currentTerm);

//                int voteGrantedNum = (int) peerMap.values().stream()
//                        .filter(x -> x.getVoteGranted() != null && x.getVoteGranted())
//                        .count();
                int voteGrantedNum = peers.getVoteGrantedNum();

                logger.info("voteGrantedNum={}", voteGrantedNum);
                if (voteGrantedNum > configuration.getServersCount() / 2) {
                    logger.info("Got majority vote, serverId={} become leader", localServer.getId());
                    startVote();
                }
            } else {
                logger.info("pre vote denied by server {} with term {}, my term is {}",
                        peer.getServer().getId(), response.getTerm(), currentTerm);
            }
        } finally {
            lock.unlock();
        }
    }

    private void requestVote(Peer peer) {
        logger.info("begin vote request");

        RaftMessage.VoteRequest.Builder requestBuilder = RaftMessage.VoteRequest.newBuilder();
        lock.lock();
        try {
            peer.setVoteGranted(null);
            requestBuilder.setCandidateId(localServer.getId())
                    .setTerm(currentTerm)
                    .setLastLogIndex(log.getLastLogIndex())
                    .setLastLogTerm(getLastLogTerm());
        } finally {
            lock.unlock();
        }

        RaftMessage.VoteRequest request = requestBuilder.build();
        RaftMessage.VoteResponse voteResponse;
        try {
            voteResponse = peer.getRpcClient().requestVote(request);
        } catch (Exception e) {
            logger.error("request vote with peer[" + peer.getServer().getEndPoint().toString() + "] failed", e);
            peer.setVoteGranted(false);

            return;
        }

        lock.lock();
        try {
            boolean voteGranted = voteResponse.getVoteGranted();
            if (currentTerm != request.getTerm() || state != NodeState.CANDIDATE) {
                logger.info("ignore requestVote RPC result");
                return;
            }

            if (voteResponse.getTerm() > currentTerm) {
                logger.info("Received RequestVote response from server {} in term {} (this server's term was {})",
                        peer.getServer().getId(), voteResponse.getTerm(), currentTerm);
                stepDown(voteResponse.getTerm());
                return;
            }

            if (voteGranted) {
                logger.info("Got vote from server {} for term {}", peer.getServer().getId(), currentTerm);
                int voteGrantedNum = 0;
                if (votedFor == localServer.getId()) {
                    voteGrantedNum += 1;
                }

//                voteGrantedNum += (int) peerMap.values().stream()
//                        .filter(x -> x.getVoteGranted() != null && x.getVoteGranted())
//                        .count();
                voteGrantedNum += peers.getVoteGrantedNum();

                logger.info("voteGrantedNum={}", voteGrantedNum);
                if (voteGrantedNum > configuration.getServersCount() / 2) {
                    logger.info("Got majority vote, serverId={} become leader", localServer.getId());
                    becomeLeader();
                }
            } else {
                logger.info("Vote denied by server {} with term {}, my term is {}", peer.getServer().getId(), voteResponse.getTerm(), currentTerm);
            }
        } finally {
            lock.unlock();
        }
    }

    public long getLastLogTerm() {
        long lastLogIndex = log.getLastLogIndex();
        if (lastLogIndex >= log.getFirstLogIndex()) {
            return log.getEntryTerm(lastLogIndex);
        } else {
            // log为空，lastLogIndex == lastSnapshotIndex
            return 0; // snapshot.getMetaData().getLastIncludedTerm();
        }
    }

    /**
     * 卸任
     *
     * @param newTerm
     */
    public void stepDown(long newTerm) {
        if (currentTerm > newTerm) {
            logger.error("can't be happened");
            return;
        }

        if (currentTerm < newTerm) {
            currentTerm = newTerm;
            leaderId = 0;
            votedFor = 0;
            log.getMetadata().save(currentTerm, votedFor, null);
        }

        state = NodeState.FOLLOWER;
        // stop heartbeat
        if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
            heartbeatScheduledFuture.cancel(true);
        }

        resetElectionTimer();
    }

    private void becomeLeader() {
        state = NodeState.LEADER;
        leaderId = localServer.getId();
        // stop vote timer
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(true);
        }

        // start heartbeat timer
//        startNewHeartbeat();
    }

    class LeaderElectionService {
        private LeaderElection leaderElection;

        public LeaderElectionService(LeaderElection leaderElection) {
            this.leaderElection = leaderElection;
        }

        public RaftMessage.VoteResponse preVote(RaftMessage.VoteRequest request) {
            leaderElection.lock.lock();
            try {
                RaftMessage.VoteResponse.Builder responseBuilder = RaftMessage.VoteResponse.newBuilder();
                responseBuilder.setVoteGranted(false);
                responseBuilder.setTerm(leaderElection.currentTerm);

                if (request.getTerm() < leaderElection.currentTerm) {
                    return responseBuilder.build();
                }

                long lastLogTerm = leaderElection.getLastLogTerm();
                boolean canGranted = request.getLastLogTerm() > lastLogTerm ||
                        (request.getLastLogTerm() == lastLogTerm && request.getLastLogIndex() >= leaderElection.log.getLastLogIndex());
                if (canGranted) {
                    responseBuilder.setVoteGranted(true);
                    responseBuilder.setTerm(leaderElection.currentTerm);

                    logger.info("preVote request from server {} in term {} (my term is {}), granted={}",
                            request.getCandidateId(), request.getTerm(), leaderElection.currentTerm, responseBuilder.getVoteGranted());
                }

                return responseBuilder.build();
            } finally {
                leaderElection.lock.unlock();
            }
        }

        public RaftMessage.VoteResponse requestVote(RaftMessage.VoteRequest request) {
            leaderElection.lock.lock();
            try {
                RaftMessage.VoteResponse.Builder responseBuilder = RaftMessage.VoteResponse.newBuilder();
                responseBuilder.setVoteGranted(false);
                responseBuilder.setTerm(leaderElection.currentTerm);

                if (request.getTerm() < leaderElection.currentTerm) {
                    return responseBuilder.build();
                }

                if (request.getTerm() > leaderElection.currentTerm) {
                    leaderElection.stepDown(request.getTerm());
                }

                long lastLogTerm = leaderElection.getLastLogTerm();
                boolean canGranted = request.getLastLogTerm() > lastLogTerm ||
                        (request.getLastLogTerm() == lastLogTerm && request.getLastLogIndex() >= leaderElection.log.getLastLogIndex());
                if (canGranted && leaderElection.votedFor == 0) {
                    leaderElection.stepDown(request.getTerm());
                    leaderElection.votedFor = request.getCandidateId();
                    leaderElection.log.getMetadata().save(leaderElection.currentTerm, leaderElection.votedFor, null);

                    responseBuilder.setVoteGranted(true);
                    responseBuilder.setTerm(leaderElection.currentTerm);

                    logger.info("RequestVote request from server {} in term {} (my term is {}), granted={}",
                            request.getCandidateId(), request.getTerm(), leaderElection.currentTerm, responseBuilder.getVoteGranted());
                }

                return responseBuilder.build();
            } finally {
                leaderElection.lock.unlock();
            }
        }

        public RaftMessage.AppendEntriesResponse appendEntries(RaftMessage.AppendEntriesRequest request) {
            leaderElection.lock.lock();
            try {
                RaftMessage.AppendEntriesResponse.Builder responseBuilder = RaftMessage.AppendEntriesResponse.newBuilder();
                responseBuilder.setTerm(leaderElection.currentTerm);
                responseBuilder.setResCode(RaftMessage.ResCode.RES_CODE_FAIL);
                responseBuilder.setLastLogIndex(leaderElection.log.getLastLogIndex());

                if (request.getTerm() < leaderElection.currentTerm) {
                    return responseBuilder.build();
                }

                // 接受日志，当前状态一定不是leader，做卸任检测操作
                leaderElection.stepDown(request.getTerm());

                if (leaderElection.leaderId == 0) {
                    leaderElection.leaderId = request.getLeaderId();
                    logger.info("new leaderId={}", leaderElection.leaderId);
                }

                // TODO 确认逻辑：任期不一致，自动加一，重新做选举
                if (leaderElection.leaderId != request.getLeaderId()) {
                    logger.warn("Another peer={} declares that it is the leader at term={} which was occupied by leader={}",
                            request.getLeaderId(), request.getTerm(), leaderElection.leaderId);
                    leaderElection.stepDown(request.getTerm() + 1);
                    responseBuilder.setResCode(RaftMessage.ResCode.RES_CODE_FAIL);
                    responseBuilder.setTerm(request.getTerm() + 1);
                    return responseBuilder.build();
                }

                // 如果对方最后日志的前一个索引比本机最后日志索引大，说明日志中间有丢失，拒绝本次请求
                if (request.getPrevLogIndex() > leaderElection.log.getLastLogIndex()) {
                    logger.info("Rejecting AppendEntries RPC would leave gap, request prevLogIndex={}, my lastLogIndex={}",
                            request.getPrevLogIndex(), leaderElection.log.getLastLogIndex());
                    return responseBuilder.build();
                }

                // 验证最后一条日志的term是否一致
                if (request.getPrevLogIndex() >= leaderElection.log.getFirstLogIndex()
                        && leaderElection.log.getEntryTerm(request.getPrevLogIndex()) != request.getPrevLogTerm()) {
                    logger.info("Rejecting AppendEntries RPC: terms don't agree, request prevLogTerm={} in prevLogIndex={}, my is {}",
                            request.getPrevLogTerm(), request.getPrevLogIndex(),
                            leaderElection.log.getEntryTerm(request.getPrevLogIndex()));

                    if(request.getPrevLogIndex() <= 0) {
                        throw new IllegalArgumentException("request prevLogIndex <= 0");
                    }

                    responseBuilder.setLastLogIndex(request.getPrevLogIndex() - 1);
                    return responseBuilder.build();
                }

                if (request.getEntriesCount() == 0) {
                    logger.debug("heartbeat request from peer={} at term={}, my term={}", request.getLeaderId(), request.getTerm(), leaderElection.currentTerm);

                    responseBuilder.setResCode(RaftMessage.ResCode.RES_CODE_SUCCESS);
                    responseBuilder.setTerm(leaderElection.currentTerm);
                    responseBuilder.setLastLogIndex(leaderElection.log.getLastLogIndex());
                    advanceCommitIndex(request);
                    return responseBuilder.build();
                }


                List<RaftMessage.LogEntry> entries = new ArrayList<>();
                long index = request.getPrevLogIndex();
                for (RaftMessage.LogEntry entry : request.getEntriesList()) {
                    index++;

                    // 对于已经确认无误的segment日志， 对方存在落后的日志，跳过
                    if (index < leaderElection.log.getFirstLogIndex()) {
                        continue;
                    }

                    // 对于当前segment内可能存在争议日志
                    if (leaderElection.log.getLastLogIndex() >= index) {
                        // 日志任期一致，可以跳过
                        if (leaderElection.log.getEntryTerm(index) == entry.getTerm()) {
                            continue;
                        }

                        // 日志任期不一致，删除本地争议索引后的所有数据
                        long lastIndexKept = index - 1;
                        leaderElection.log.truncateSuffix(lastIndexKept);
                    }

                    entries.add(entry);
                }

                leaderElection.log.append(entries);
                leaderElection.log.getMetadata().save(leaderElection.currentTerm, null, leaderElection.log.getLastLogIndex());

                advanceCommitIndex(request);

                logger.info("AppendEntries request from server {}  in term {} (my term is {}), entryCount={}",
                        request.getLeaderId(), request.getTerm(), leaderElection.currentTerm, request.getEntriesCount());

                responseBuilder.setResCode(RaftMessage.ResCode.RES_CODE_SUCCESS);
                responseBuilder.setLastLogIndex(leaderElection.log.getLastLogIndex());

                return responseBuilder.build();
            } finally {
                leaderElection.lock.unlock();
            }
        }

        private void advanceCommitIndex(RaftMessage.AppendEntriesRequest request) {
            leaderElection.commitIndex = Math.min(request.getLeaderCommit(), request.getPrevLogIndex() + request.getEntriesCount());

            if (leaderElection.lastApplied < leaderElection.commitIndex) {
                for(long index = leaderElection.lastApplied + 1; index < leaderElection.commitIndex; index ++) {
                    RaftMessage.LogEntry entry = leaderElection.log.getEntry(index);
                    if (entry != null) {
//                        if (entry.getType() == RaftMessage.EntryType.ENTRY_TYPE_DATA) {
                            leaderElection.stateMachine.apply(entry.getData().toByteArray());
//                        } else if (entry.getType() == RaftMessage.EntryType.ENTRY_TYPE_CONFIGURATION) {
//                            raftNode.applyConfiguration(entry);
//                        }
                    }

                    leaderElection.lastApplied = index;
                }
            }
        }

        public RaftMessage.InstallSnapshotResponse installSnapshot(RaftMessage.InstallSnapshotRequest request) {
            return null;
        }
    }
}
