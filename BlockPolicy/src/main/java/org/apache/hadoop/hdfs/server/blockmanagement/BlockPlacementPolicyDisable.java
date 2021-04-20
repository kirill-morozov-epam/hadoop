package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ZKFailoverController;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public class BlockPlacementPolicyDisable extends BlockPlacementPolicyRackFaultTolerant implements Watcher {

    public static final String HDFS_BLOCK_REPLICATION_FLAG_PATH = "/hdfs_block_replication_flag";
    public static final int ZERO_SYMBOL_BYTE = 48;
    private ZooKeeper zk;

    @Override
    public DatanodeStorageInfo[] chooseTarget(String srcPath,
                                              int numOfReplicas,
                                              Node writer,
                                              List<DatanodeStorageInfo> chosenNodes,
                                              boolean returnChosenNodes,
                                              Set<Node> excludedNodes,
                                              long blocksize,
                                              final BlockStoragePolicy storagePolicy,
                                              EnumSet<AddBlockFlag> flags) {
        LOG.info("BlockPlacementPolicyDisable( extends BlockPlacementPolicyRackFaultTolerant ) is applied for " + srcPath);
//        LOG.info("BlockPlacementPolicyDisable numOfReplicas are " + numOfReplicas);
//        LOG.info("BlockPlacementPolicyDisable writer is " + writer.toString());
//        LOG.info("BlockPlacementPolicyDisable chosenNodes are " + chosenNodes);
//        LOG.info("BlockPlacementPolicyDisable excludedNodes are " + excludedNodes);
//        LOG.info("BlockPlacementPolicyDisable storagePolicy is " + storagePolicy);
        LOG.info("BlockPlacementPolicyDisable flags are " + flags);

        if (isHDFSBlockReplicationFlagBlocked() && flags == null) {
            LOG.info("BlockPlacementPolicyDisable is applied");
            return DatanodeStorageInfo.EMPTY_ARRAY;
        } else {
            LOG.info("BlockPlacementPolicyDisable is skipped");
            return super.chooseTarget(srcPath, numOfReplicas, writer, chosenNodes, returnChosenNodes,
                    excludedNodes, blocksize, storagePolicy, flags);
        }
    }


    @Override
    public void initialize(Configuration conf, FSClusterStats stats,
                           NetworkTopology clusterMap,
                           Host2NodesMap host2datanodeMap) {
        super.initialize(conf, stats, clusterMap, host2datanodeMap);
        initZkClient(conf.get(ZKFailoverController.ZK_QUORUM_KEY));
    }

    private void initZkClient(String hostPort) {
        try {
            LOG.info("Init zookeeper with hostPort: "+hostPort);
            zk = new ZooKeeper(hostPort, 3000, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean isHDFSBlockReplicationFlagBlocked() {
        byte[] flag;
        try {
            flag = zk.getData(HDFS_BLOCK_REPLICATION_FLAG_PATH, this, null);
        } catch (KeeperException | InterruptedException e) {
            LOG.info("The node " + HDFS_BLOCK_REPLICATION_FLAG_PATH + " is absent in the zookeeper or process is interrupted");
            return false;
        }
        if (flag == null) {
            LOG.info("HDFS_BLOCK_REPLICATION_FLAG is null");
            return false;
        }
        if (flag[0] == ZERO_SYMBOL_BYTE) {
            LOG.info("HDFS_BLOCK_REPLICATION_FLAG is set to BLOCK");
            return true;
        } else {
            LOG.info("HDFS_BLOCK_REPLICATION_FLAG is set to RELEASE");
            return false;
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
    }
}
