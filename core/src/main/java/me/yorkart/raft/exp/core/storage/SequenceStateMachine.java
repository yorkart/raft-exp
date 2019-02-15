package me.yorkart.raft.exp.core.storage;

import me.yorkart.raft.exp.core.consensus.StateMachine;

/**
 * @author wangyue1
 * @date 2019/2/15
 */
public class SequenceStateMachine implements StateMachine {

    private String dataDir;

    public SequenceStateMachine(String dataDir) {
        this.dataDir = dataDir;
    }

    @Override
    public void writeSnapshot(String snapshotDir) {

    }

    @Override
    public void readSnapshot(String snapshotDir) {

    }

    @Override
    public void apply(byte[] dataBytes) {

    }
}
