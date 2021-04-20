package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestBlockPlacementPolicyDisable extends BaseReplicationPolicyTest{

    public TestBlockPlacementPolicyDisable(){
        System.out.println("TestReplicationPolicyEvenDC call");
        this.blockPlacementPolicy = BlockPlacementPolicyDisable.class.getName();
    }

    @Override
    DatanodeDescriptor[] getDatanodeDescriptors(Configuration conf) {
        final String[] racks = {
                "/d1/r1/n1",
                "/d1/r1/n1",
                "/d1/r2/n2",
                "/d1/r2/n2",
                "/d2/r3/n3",
                "/d2/r3/n3",
                "/d2/r4/n4",
                "/d2/r4/n4",
        };
        final String[] hosts = {
                "h1",
                "h2",
                "h3",
                "h4",
                "h5",
                "h6",
                "h7",
                "h8"
        };

        storages = DFSTestUtil.createDatanodeStorageInfos(racks, hosts);
        return DFSTestUtil.toDatanodeDescriptor(storages);
    }

    @Test
    public void testChooseTarget1() throws Exception {
        updateHeartbeatWithUsage(dataNodes[0],
                2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
                HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
                0L, 0L, 4, 0); // overloaded

        DatanodeStorageInfo[] targets;

        targets = chooseTarget(4);
        assertEquals(targets.length, 4);
        assertEquals(storages[0], targets[0]);
        assertFalse(isOnSameRack(targets[0], targets[1]));
        assertFalse(isOnSameRack(targets[1], targets[2]));
        assertFalse(isOnSameRack(targets[2], targets[3]));
        assertFalse(isOnSameRack(targets[0], targets[2]));
        assertFalse(isOnSameRack(targets[0], targets[3]));
        assertFalse(isOnSameRack(targets[1], targets[3]));

        updateHeartbeatWithUsage(dataNodes[0],
                2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
                HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }
}
