package me.yorkart.raft.exp.core.consensus;

import me.yorkart.raft.exp.core.proto.RaftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author wangyue1
 * @date 2019/2/22
 */
public class LeaderElectionV2 {
    private static Logger logger = LoggerFactory.getLogger(LeaderElectionV2.class);

    interface VoteEventCallback {
        void onVotedInvalid(int answerPeerCount);

        void onHigherTerm(RaftMessage.VoteResponse maxTermResponse);

        void onVotedGrantedSuccess(List<RaftMessage.VoteResponse> responses);

        void onPreVoteGrantedFailure(RaftMessage.VoteResponse response);
    }

    interface VoteEvent {
        void onVotedInvalid(int answerPeerCount);

        void onHigherTerm(RaftMessage.VoteResponse maxTermResponse);

        void onVotedSuccess(List<RaftMessage.VoteResponse> responses);

        void onVotedFailure(int voteCount);
    }

    private Lock lock = new ReentrantLock();
    private Condition awaitCondition = lock.newCondition();

    private ExecutorService executorService;

    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture electionScheduledFuture;

    private final RaftMessage.Server localServer;
    private final Peers peers;
    private RaftMessage.VoteRequest request;
    private final int electionTimeout;

    private VoteEvent voteEventCallback;
    private NodeState nodeState;

    public LeaderElectionV2(RaftMessage.Server localServer, Peers peers, RaftMessage.VoteRequest request, int electionTimeout) {
        this.localServer = localServer;
        this.peers = peers;
        this.request = request;
        this.electionTimeout = electionTimeout;

        // init thread pool
        executorService = new ThreadPoolExecutor(
                peers.size(),
                peers.size(),
                30,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() {
        resetElectionTimer();
    }

    private void resetElectionTimer() {
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(true);
        }

        electionScheduledFuture = scheduledExecutorService.schedule(
                ()-> {
                    lock.lock();
                    try {
                        startPreVote();
                    } finally {
                        lock.unlock();
                    }
                },
                getElectionTimeoutMs(),
                TimeUnit.MILLISECONDS);
    }

    private int getElectionTimeoutMs() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int randomElectionTimeout = electionTimeout + random.nextInt(0, electionTimeout);
        logger.debug("new election time is after {} ms", randomElectionTimeout);
        return randomElectionTimeout;
    }

    public void startPreVote() {
        nodeState = NodeState.PRE_CANDIDATE;
        vote(peer -> peer.getRpcClient().preVote(request), new VoteEvent() {
            @Override
            public void onVotedInvalid(int answerPeerCount) {
                voteEventCallback.onVotedInvalid(answerPeerCount);
                resetElectionTimer();
            }

            @Override
            public void onHigherTerm(RaftMessage.VoteResponse maxTermResponse) {
                voteEventCallback.onHigherTerm(maxTermResponse);
                resetElectionTimer();
            }

            @Override
            public void onVotedSuccess(List<RaftMessage.VoteResponse> responses) {
                startVote();
            }

            @Override
            public void onVotedFailure(int voteCount) {
                voteEventCallback.onVotedFailure(voteCount);
                resetElectionTimer();
            }
        });
    }

    private void startVote() {
        nodeState = NodeState.CANDIDATE;
        request = RaftMessage.VoteRequest.newBuilder(request).setTerm(request.getTerm()+1).build();
        vote(peer -> peer.getRpcClient().requestVote(request), new VoteEvent() {
            @Override
            public void onVotedInvalid(int answerPeerCount) {
                voteEventCallback.onVotedInvalid(answerPeerCount);
            }

            @Override
            public void onHigherTerm(RaftMessage.VoteResponse maxTermResponse) {
                voteEventCallback.onHigherTerm(maxTermResponse);

            }

            @Override
            public void onVotedSuccess(List<RaftMessage.VoteResponse> responses) {
                voteEventCallback.onVotedSuccess(responses);
            }

            @Override
            public void onVotedFailure(int voteCount) {
                voteEventCallback.onVotedFailure(voteCount);
            }
        });

        resetElectionTimer();
    }

    private void vote(final Function<Peer, RaftMessage.VoteResponse> voteTask, VoteEvent voteEvent) {
        List<Future<RaftMessage.VoteResponse>> futures = new ArrayList<>();
        for (final Peer peer : peers.getPeers()) {
            Future<RaftMessage.VoteResponse> future = executorService.submit(() -> voteTask.apply(peer));
            futures.add(future);
        }

        long timeout = 3000;
        long beginTimeout = System.currentTimeMillis();

        List<RaftMessage.VoteResponse> responses = new ArrayList<>();
        for (Future<RaftMessage.VoteResponse> future : futures) {
            try {
                RaftMessage.VoteResponse response = future.get(timeout, TimeUnit.MICROSECONDS);
                responses.add(response);
            } catch (InterruptedException e) {
                logger.error(msg("vote task interrupted"), e);
            } catch (ExecutionException e) {
                logger.error(msg("vote task error"), e);
            } catch (TimeoutException e) {
                logger.warn(msg("vote task timeout error"), e);
            }

            long endTimeout = System.currentTimeMillis();
            long timeUsed = endTimeout - beginTimeout;
            timeout = timeout - timeUsed;
            beginTimeout = endTimeout;
        }

        int half = (peers.size() + 1) / 2;

        // 未超过半数结果
        if (responses.size() <= half) {
            logger.warn(msg("vote invalid, response count " + responses.size()));
            voteEvent.onVotedInvalid(responses.size());
            return;
        }

        RaftMessage.VoteResponse maxTermResponse = responses.stream().sorted((r1 , r2) -> (int)(r1.getTerm() - r2.getTerm())).collect(Collectors.toList()).get(0);
//        long maxTerm = responses.stream().mapToLong(RaftMessage.VoteResponse::getTerm).max().orElse(0L);
        if (maxTermResponse.getTerm() > request.getTerm()) {
            logger.info(msg("find higher term " + maxTermResponse.getTerm() + ", current term " + request.getTerm()));
            voteEvent.onHigherTerm(maxTermResponse);
            return;
        }

        int voteGranted = responses.stream().mapToInt(response -> response.getVoteGranted() ? 1 : 0).sum();
        if (voteGranted <= half) {
            logger.info(msg("vote failure, granted count " + voteGranted));
            voteEvent.onVotedFailure(voteGranted);
        } else {
            logger.info(msg("vote success"));
            voteEvent.onVotedSuccess(responses);
        }
    }

    private String msg(String log) {
        return "[" + nodeState + "]" + log;
    }
}
