package me.yorkart.raft.exp.core.consensus;

public enum NodeState {
    FOLLOWER,
    PRE_CANDIDATE,
    CANDIDATE,
    LEADER
}
