package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ha.ZKFailoverController;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class BlockPlacementPolicyEvenDC extends BlockPlacementPolicyDefault implements Watcher {

    public static final String HDFS_DC_NAME_CONTAIN = "dc";
    public static final int DC_NUMBERS = 2;
    public static final String HDFS_BLOCK_REPLICATION_FLAG_PATH = "/hdfs_block_replication_flag";
    public static final int ZERO_SYMBOL_BYTE = 48;
    private ZooKeeper zk;


    @Override
    public DatanodeStorageInfo[] chooseTarget(String srcPath, int numOfReplicas, Node writer, List<DatanodeStorageInfo> chosen, boolean returnChosenNodes, Set<Node> excludedNodes, long blocksize, BlockStoragePolicy storagePolicy, EnumSet<AddBlockFlag> flags) {
        LOG.info("BlockPlacementPolicyEvenDC is applied for " + srcPath);
        LOG.info("BlockPlacementPolicyEvenDC numOfReplicas are " + numOfReplicas);
        LOG.info("BlockPlacementPolicyEvenDC writer is " + writer.toString());
        LOG.info("BlockPlacementPolicyEvenDC chosenNodes are " + chosen);
        LOG.info("BlockPlacementPolicyEvenDC returnChosenNodes are " + returnChosenNodes);
        LOG.info("BlockPlacementPolicyEvenDC excludedNodes are " + excludedNodes);
        LOG.info("BlockPlacementPolicyEvenDC blocksize is " + blocksize);
        LOG.info("BlockPlacementPolicyEvenDC storagePolicy is " + storagePolicy);
        LOG.info("BlockPlacementPolicyEvenDC flags are " + flags);
        if (isHDFSBlockReplicationFlagBlocked() && flags == null) {
            LOG.info("BlockPlacementPolicyEvenDC is applied");
            return DatanodeStorageInfo.EMPTY_ARRAY;
        } else {
            LOG.info("BlockPlacementPolicyEvenDC is skipped");
            return getPlacementEvenDC(numOfReplicas, storagePolicy, excludedNodes, blocksize);
        }
    }

    private DatanodeStorageInfo[] getPlacementEvenDC(int numOfReplicas, BlockStoragePolicy storagePolicy, Set<Node> excludedNodes, long blocksize) {
        //TODO get right storage type from list
        EnumMap<StorageType, Integer> storageTypes = getRequiredStorageTypes(storagePolicy.chooseStorageTypes((short) numOfReplicas));
        int numOfNodesInDC = (excludedNodes.size() + numOfReplicas) / DC_NUMBERS;
        return clusterMap.getDatanodesInRack("").stream().
                filter(node -> node.getName().contains(HDFS_DC_NAME_CONTAIN)).
                flatMap(node -> getNodesInDC(node, numOfNodesInDC, excludedNodes, blocksize, storageTypes).stream())
                .toArray(DatanodeStorageInfo[]::new);
    }

    private List<DatanodeStorageInfo> getNodesInDC(Node dc, int requiredReplicas, Set<Node> excludedNodes, long blocksize, EnumMap<StorageType, Integer> storageTypes) {
        ArrayList<DatanodeStorageInfo> result = new ArrayList<>();
        HashSet<Node> toExclude = new HashSet<>(excludedNodes);
        int requiredNodes = getRequiredNumReplicaInDC(dc, requiredReplicas, excludedNodes);
        LOG.info("BlockPlacementPolicyEvenDC:getNodesInDC choose in " + NodeBase.getPath(dc));
        LOG.info("BlockPlacementPolicyEvenDC:getNodesInDC requiredNodes is " + requiredNodes);
        LOG.info("BlockPlacementPolicyEvenDC:getNodesInDC excludedNodes are " + excludedNodes);
        LOG.info("BlockPlacementPolicyEvenDC:getNodesInDC blocksize is " + blocksize);
        LOG.info("BlockPlacementPolicyEvenDC:getNodesInDC storageTypes is " + storageTypes);
        for (int i = 0; i < requiredNodes; i++) {
            Node node = clusterMap.chooseRandom(NodeBase.getPath(dc), toExclude);
            if (node != null) {
                DatanodeStorageInfo chosenStorageBlock = chooseStorage4Block((DatanodeDescriptor) node, blocksize, new ArrayList<DatanodeStorageInfo>(), storageTypes);
                if(chosenStorageBlock != null) {
                    result.add(chosenStorageBlock);
                    toExclude.addAll(clusterMap.getDatanodesInRack(node.getNetworkLocation()));
                }
            }
        }
        LOG.info("BlockPlacementPolicyEvenDC:getNodesInDC result are " + result);
        return result;
    }

    private DatanodeStorageInfo chooseStorage4Block(DatanodeDescriptor dnd,
                                                    long blockSize,
                                                    List<DatanodeStorageInfo> results,
                                                    EnumMap<StorageType, Integer> storageTypes){
        for (Iterator<Map.Entry<StorageType, Integer>> iter = storageTypes
                .entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<StorageType, Integer> entry = iter.next();
            DatanodeStorageInfo localStorage = chooseStorage4Block(
                    dnd, blockSize, results, entry.getKey());
            if (localStorage != null) {
                int num = entry.getValue();
                if (num == 1) {
                    iter.remove();
                } else {
                    entry.setValue(num - 1);
                }
                return localStorage;
            }
        }
//        for (StorageType storageType:storageTypes) {
//            DatanodeStorageInfo storage = chooseStorage4Block(
//                    dnd, blockSize, results, storageType);
//            if (storage != null) {
//                return storage;
//            }
//        }
        return null;
    }

    private EnumMap<StorageType, Integer> getRequiredStorageTypes(
            List<StorageType> types) {
        EnumMap<StorageType, Integer> map = new EnumMap<>(StorageType.class);
        for (StorageType type : types) {
            if (!map.containsKey(type)) {
                map.put(type, 1);
            } else {
                int num = map.get(type);
                map.put(type, num + 1);
            }
        }
        return map;
    }

    private int getRequiredNumReplicaInDC(Node dc, int requiredReplicas4DC, Set<Node> excludedNodes) {
        String dcName = NodeBase.getPath(dc);
        for (Node alreadyPlaced : excludedNodes) {
            if (NodeBase.getPath(alreadyPlaced).contains(dcName)) {
                requiredReplicas4DC--;
            }
        }
        LOG.info("BlockPlacementPolicyEvenDC:getRequiredNumReplicaInDC requiredReplicas4DC is " + requiredReplicas4DC);
        return requiredReplicas4DC;
    }

    private List<String> getDCs() {
        return clusterMap.getDatanodesInRack("").stream().
                filter(node -> node.getName().contains(HDFS_DC_NAME_CONTAIN)).
                map(node -> NodeBase.getPath(node)).
                collect(Collectors.toList());
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
            LOG.info("Init zookeeper with hostPort: " + hostPort);
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
