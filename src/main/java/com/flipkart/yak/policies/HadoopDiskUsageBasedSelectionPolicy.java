package com.flipkart.yak.policies;

import com.flipkart.yak.commons.ConnectionInventory;
import com.flipkart.yak.commons.hadoop.HDFSDefaults;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionSchedule;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.tools.DFSAdmin;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;

@Slf4j
public class HadoopDiskUsageBasedSelectionPolicy extends BasePolicy {

    private static final String DISK_NAMENODE_KEY = "compactor.namenode.host";
    private static final int MAX_NUMBER_OF_NAMENODES = 2;
    private static final List<String> nameNodes = new ArrayList<>(MAX_NUMBER_OF_NAMENODES);
    private static final String DFS_CLIENT_USE_HOST = "dfs.client.use.datanode.hostname";

    public static void main(String[] args) {
        log.info(args[0]);
        System.setProperty("HADOOP_USER_NAME", "yak");
        HadoopDiskUsageBasedSelectionPolicy hadoopDiskUsageBasedSelectionPolicy = new HadoopDiskUsageBasedSelectionPolicy();
        CompactionSchedule compactionSchedule = new CompactionSchedule(10,12);
        CompactionContext compactionContext = new CompactionContext(args[0], compactionSchedule,"dummy");
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(getHbaseConfig(compactionContext));
            if (connection != null) {
                hadoopDiskUsageBasedSelectionPolicy.initDFSClientFromConnection(connection);
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        }

    }

    private static Configuration getHbaseConfig(CompactionContext context) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.clear();
        log.info("creating hbase config with {}", context.getClusterID());
        configuration.set("hbase.zookeeper.quorum", ConnectionInventory.getZookeeperQuorum(context.getClusterID()));
        configuration.set("zookeeper.znode.parent", ConnectionInventory.getParentFromID(context.getClusterID()));
        return configuration;
    }

    @Override
    public void setFromConfig(List<Pair<String, String>> configs) {
    }

    public void initDFSClientFromConnection(Connection connection) throws IOException {
        this.admin = connection.getAdmin();
        Configuration configuration = new Configuration(false);
        HDFSDefaults hdfsDefaults = new HDFSDefaults();
        ServerName activeMaster = this.admin.getMaster();
        Collection<ServerName> backupMastersMasters = this.admin.getBackupMasters();
        List<ServerName> allMasters = new ArrayList<>(backupMastersMasters);
        allMasters.add(activeMaster);
        hdfsDefaults.loadDefaults();
        hdfsDefaults.forEach(configuration::set);
        allMasters.forEach(e->log.info("Master Found: {}",e.getServerName()));
        configuration.set(DFS_CLIENT_USE_HOST, "true");
        Iterator<ServerName> iterator = allMasters.iterator();
        if(allMasters.size() > 0 ){
            ServerName nn1 = iterator.next();
            configuration.set(HDFSDefaults.KEY_DFS_NAMENODE_HTTP_NN_1,nn1.getHostname()+":50070");
            configuration.set(HDFSDefaults.KEY_DFS_NAMENODE_RPC_NN_1, nn1.getHostname()+":8020");
            log.info("setting {} as {}", HDFSDefaults.KEY_DFS_NAMENODE_RPC_NN_1,nn1.getHostname()+":8020");
        }

        if(allMasters.size() > 1 ){
            ServerName nn2 = iterator.next();
            configuration.set(HDFSDefaults.KEY_DFS_NAMENODE_HTTP_NN_2,nn2.getHostname()+":50070");
            configuration.set(HDFSDefaults.KEY_DFS_NAMENODE_RPC_NN_2, nn2.getHostname()+":8020");
            log.info("setting {} as {}", HDFSDefaults.KEY_DFS_NAMENODE_RPC_NN_2,nn2.getHostname()+":8020");
        }

        String namenode = DFSUtil.getNamenodeNameServiceId(configuration);
        String backupNamenode = DFSUtil.getBackupNameServiceId(configuration);
        log.info("Namenode: {}", namenode);
        log.info("Backup Namenode: {}", backupNamenode);
        Map<String, Map<String, InetSocketAddress>> nnRpcAddresses = DFSUtil.getHaNnRpcAddresses(configuration);
        for(Map.Entry<String, Map<String, InetSocketAddress>> entry : nnRpcAddresses.entrySet()) {
            log.info("ID: {}", entry.getKey());

                for (Map.Entry<String, InetSocketAddress> inets : entry.getValue().entrySet()) {
                    log.info("Inet {}", inets.getKey());
                    try {
                        DFSClient dfsClient = new DFSClient(inets.getValue(), configuration);
                        ClientProtocol namenodeProtocol = dfsClient.getNamenode();
                        DatanodeStorageReport[] datanodeStorageReports = namenodeProtocol.getDatanodeStorageReport(HdfsConstants.DatanodeReportType.LIVE);
                        for (DatanodeStorageReport datanodeStorageReport : datanodeStorageReports) {
                            DatanodeInfo datanodeInfo = datanodeStorageReport.getDatanodeInfo();
                            log.info("{} : {} : {}", datanodeInfo.getHostName(), datanodeInfo.getDfsUsed()/(1024L*1024L*1024L), datanodeInfo.getDfsUsedPercent());
                        }
                    } catch (ConnectException e){
                        log.error("could not connect: {}",e.getMessage());
                    }
                }

        }

    }


    private void release() {

    }


    @Override
    List<String> getEligibleRegions(Map<String, List<String>> regionFNHostnameMapping, Set<String> compactingRegions, List<RegionInfo> allRegions) throws IOException {
        return null;
    }
}
