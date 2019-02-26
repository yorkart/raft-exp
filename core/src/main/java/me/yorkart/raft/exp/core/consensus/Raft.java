package me.yorkart.raft.exp.core.consensus;

import me.yorkart.raft.exp.core.storage.Log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author wangyue1
 * @date 2019/1/31
 */
public class Raft {
    private static Logger logger = LoggerFactory.getLogger(Raft.class);

    /**
     * Persistent state on all servers
     */
    // 服务器最后一次知道的任期号（初始化为 0，持续递增）
    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    private long currentTerm;
    // 在当前获得选票的候选人的Id
    // candidateId that received vote in current term (or null if none)
    private int votedFor;
    // 日志条目；每个日志条目包含状态机的命令，以及leader接收日志条目是的term
    // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
    private Log log;

    /**
     * Volatile state on all servers
     */
    // 已知提交的最高日志条目索引（初始0，持续递增）
    // index of highest log entry known to be committed (initialized to 0, increases monotonically)
    private int commitIndex;
    // 应用于状态机的最高日志条目索引（初始0，持续递增）
    // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    private int lastApplied;

    private int leaderId; // leader节点id
    private NodeState state = NodeState.FOLLOWER;
    private Lock lock = new ReentrantLock();

    void becomeLeader() {
        state = NodeState.LEADER;

    }

    void becomePreCandidate() {
        state = NodeState.PRE_CANDIDATE;
    }

    void becomeCandidate() {
        state = NodeState.CANDIDATE;
    }

    void becomeFollow() {
        state = NodeState.FOLLOWER;
    }

    void stepDown(long newTerm) {
        if (currentTerm > newTerm) {
            logger.error("can't be happened");
            return;
        }

        if (currentTerm < newTerm) {
            currentTerm = newTerm;
        }

        state = NodeState.FOLLOWER;
    }
}
