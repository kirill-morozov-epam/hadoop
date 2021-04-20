package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;

public class TestReplicationPolicyEvenDC extends BaseReplicationPolicyTest {

    public TestReplicationPolicyEvenDC(){
        System.out.println("TestReplicationPolicyEvenDC call");
//        this.blockPlacementPolicy = BlockPlacementPolicyRackFaultTolerant.class.getName();
//        this.blockPlacementPolicy = BlockPlacementPolicyDefault.class.getName();
        this.blockPlacementPolicy = BlockPlacementPolicyEvenDC.class.getName();
    }

    @Override
    DatanodeDescriptor[] getDatanodeDescriptors(Configuration conf) {
//        conf.setBoolean(DFS_USE_DFS_NETWORK_TOPOLOGY_KEY, false);
//        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_IMPL_KEY,
//                NetworkTopologyWithNodeGroup.class.getName());
        final String[] racks = {
                "/dc1/r10",
                "/dc1/r10",
                "/dc1/r11",
                "/dc1/r11",
                "/dc1/r20",
                "/dc1/r20",
                "/dc1/r21",
                "/dc1/r21",
                "/dc2/r30",
                "/dc2/r30",
                "/dc2/r31",
                "/dc2/r31",
                "/dc2/r40",
                "/dc2/r40",
                "/dc2/r41",
                "/dc2/r41"
        };
        final String[] hosts = {
                "h1",
                "h2",
                "h3",
                "h4",
                "h5",
                "h6",
                "h7",
                "h8",
                "h9",
                "h10",
                "h11",
                "h12",
                "h13",
                "h14",
                "h15",
                "h16"
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
//        assertEquals(storages[0], targets[0]);
        assertFalse(isOnSameRack(targets[0], targets[1]));
        assertFalse(isOnSameRack(targets[1], targets[2]));
        assertFalse(isOnSameRack(targets[2], targets[3]));
        assertFalse(isOnSameRack(targets[0], targets[2]));
        assertFalse(isOnSameRack(targets[0], targets[3]));
        assertFalse(isOnSameRack(targets[1], targets[3]));

//
//        assertTrue(isOnSameRack(targets[1], targets[2]) ||
//                isOnSameRack(targets[2], targets[3]));
//        assertFalse(isOnSameRack(targets[0], targets[2]));
//        // Make sure no more than one replicas are on the same nodegroup
//        verifyNoTwoTargetsOnSameNodeGroup(targets);

        updateHeartbeatWithUsage(dataNodes[0],
                2* HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
                HdfsServerConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }


}
