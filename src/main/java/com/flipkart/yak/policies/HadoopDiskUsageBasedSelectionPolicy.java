package com.flipkart.yak.policies;

import com.flipkart.yak.commons.ConnectionInventory;
import com.flipkart.yak.commons.HBaseUtils;
import com.flipkart.yak.commons.RegionEligibilityStatus;
import com.flipkart.yak.commons.Report;
import com.flipkart.yak.commons.hadoop.HDFSConnection;
import com.flipkart.yak.commons.hadoop.HDFSDefaults;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionSchedule;
import com.flipkart.yak.core.CompactionRuntimeException;
import com.flipkart.yak.interfaces.RegionSelectionPolicy;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
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

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * Uses {@link HDFSConnection} as resource which is essentially a collection of HADOOP and HBASE connection to derive
 * Disk Usage of target nodes which are part of given {@link CompactionContext}. This makes an admin call to HBASE to
 * derive all RegionServers falling under an RSGroup, creates a mapping of Host-To-Region and analyses disk usage of relevant
 * Hosts only. Hence IO heavy and on every run processes the entire cluster data.
 */
@Slf4j
public class HadoopDiskUsageBasedSelectionPolicy implements RegionSelectionPolicy<HDFSConnection> {

    private static final String DISK_NAMENODE_KEY = "compactor.namenode.host";
    private static final int MAX_NUMBER_OF_NAMENODES = 2;
    private static final String DFS_CLIENT_USE_HOST = "dfs.client.use.datanode.hostname";
    private static double DISK_USAGE_THRESHOLD_PERCENT = 75.0;
    private static final String KEY_DISK_USAGE_THRESHOLD_PERCENT = "compactor.max.diskusage.percentt";
    

    private static Configuration getHbaseConfig(CompactionContext context) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.clear();
        log.debug("creating hbase config with {}", context.getClusterID());
        configuration.set("hbase.zookeeper.quorum", ConnectionInventory.getZookeeperQuorum(context.getClusterID()));
        configuration.set("zookeeper.znode.parent", ConnectionInventory.getParentFromID(context.getClusterID()));
        return configuration;
    }

    @Override
    public void setFromConfig(List<Pair<String, String>> configs) {
        if(configs!= null) {
            configs.forEach(pair -> {
                if (pair.getFirst().equals(KEY_DISK_USAGE_THRESHOLD_PERCENT)) {
                    DISK_USAGE_THRESHOLD_PERCENT = Double.parseDouble(pair.getSecond());
                }
            });
        }
    }

    public ClientProtocol getNameNode(Connection connection) throws IOException {
        List<ServerName> masterServers =  this.getHBASEMasters(connection);
        Configuration configuration = this.getCoreSiteConfig(masterServers);
        Map<String, Map<String, InetSocketAddress>> nnRpcAddresses = DFSUtil.getHaNnRpcAddresses(configuration);
        for(Map.Entry<String, Map<String, InetSocketAddress>> entry : nnRpcAddresses.entrySet()) {
            for (Map.Entry<String, InetSocketAddress> inets : entry.getValue().entrySet()) {
                try {
                    DFSClient dfsClient = new DFSClient(inets.getValue(), configuration);
                    log.debug("ID:{} Inet:{} ServiceName:{}", entry.getKey(), inets.getKey(), dfsClient.getCanonicalServiceName());
                    this.testConnection(dfsClient.getNamenode());
                    return dfsClient.getNamenode();
                } catch (ConnectException e){
                    log.error("could not connect: {} !! Trying next one..",e.getMessage());
                }
            }
        }
        return null;
    }

    private void testConnection(ClientProtocol clientProtocol) throws IOException {
        clientProtocol.getStats();
    }

    private DatanodeStorageReport[] getDiskReport(ClientProtocol namenode) throws IOException {
        DatanodeStorageReport[] datanodeStorageReports = namenode.getDatanodeStorageReport(HdfsConstants.DatanodeReportType.LIVE);
        if(log.isDebugEnabled()) {
            for (DatanodeStorageReport datanodeStorageReport : datanodeStorageReports) {
                DatanodeInfo datanodeInfo = datanodeStorageReport.getDatanodeInfo();
                log.debug("Host:{} Capacity:{} UsedPercent:{}", datanodeInfo.getHostName(), datanodeInfo.getCapacity(), datanodeInfo.getDfsUsedPercent());
            }
        }
        return datanodeStorageReports;
    }

    private List<ServerName> getHBASEMasters(Connection connection)  throws IOException{
        Admin admin = connection.getAdmin();
        ServerName activeMaster = admin.getMaster();
        Collection<ServerName> backupMastersMasters = admin.getBackupMasters();
        List<ServerName> allMasters = new ArrayList<>(backupMastersMasters);
        allMasters.add(activeMaster);
        return allMasters;
    }

    private Configuration getCoreSiteConfig(List<ServerName> allMasters) {
        Configuration configuration = new Configuration(false);
        HDFSDefaults hdfsDefaults = new HDFSDefaults();
        hdfsDefaults.loadDefaults();
        hdfsDefaults.forEach(configuration::set);
        if(log.isDebugEnabled()) {
            allMasters.forEach(e -> log.debug("Master Found: {}", e.getServerName()));
        }
        configuration.set(DFS_CLIENT_USE_HOST, "true");
        Iterator<ServerName> iterator = allMasters.iterator();
        if(allMasters.size() > 0 ){
            ServerName nn1 = iterator.next();
            configuration.set(HDFSDefaults.KEY_DFS_NAMENODE_HTTP_NN_1,nn1.getHostname()+":" + HDFSDefaults.DEFAULT_NN_HTTP_PORT);
            configuration.set(HDFSDefaults.KEY_DFS_NAMENODE_RPC_NN_1, nn1.getHostname()+":"+ HDFSDefaults.DEFAULT_NN_RPC_PORT);
            log.debug("setting {} as {}", HDFSDefaults.KEY_DFS_NAMENODE_RPC_NN_1,nn1.getHostname()+":"+ HDFSDefaults.DEFAULT_NN_RPC_PORT);
        }
        if(allMasters.size() > 1 ){
            ServerName nn2 = iterator.next();
            configuration.set(HDFSDefaults.KEY_DFS_NAMENODE_HTTP_NN_2,nn2.getHostname()+":" + HDFSDefaults.DEFAULT_NN_HTTP_PORT);
            configuration.set(HDFSDefaults.KEY_DFS_NAMENODE_RPC_NN_2, nn2.getHostname()+":"+ HDFSDefaults.DEFAULT_NN_RPC_PORT);
            log.debug("setting {} as {}", HDFSDefaults.KEY_DFS_NAMENODE_RPC_NN_2,nn2.getHostname()+":"+ HDFSDefaults.DEFAULT_NN_RPC_PORT);
        }
        return configuration;
    }


    @Override
    public HDFSConnection init(CompactionContext compactionContext) {
        Connection hbase = ConnectionInventory.getInstance().get(compactionContext.getClusterID());
        ClientProtocol namenode = null;
        try {
            namenode = this.getNameNode(hbase);
        } catch (IOException e) {
            log.error("Could not create HDFS Connection: {}", e.getMessage());
        }
        return new HDFSConnection(hbase, namenode);
    }

    @Override
    public Report getReport(CompactionContext context, HDFSConnection hdfsConnection) throws CompactionRuntimeException {
        Report report = new Report(this.getClass().getName());
        try {
            Admin admin = hdfsConnection.getConnection().getAdmin();
            //TODO: This data should be populated from FavouredNode mapping - not from RSGroup info
            Map<String, Set<RegionInfo>> targetHostAndRegion = HBaseUtils.getHostToRegionMapping(context, admin);
            if(log.isDebugEnabled()) {
                targetHostAndRegion.forEach((K,V) -> log.debug("{}: {}", K, V.size()));
            }
            Connection connection = hdfsConnection.getConnection();
            if (connection != null && !connection.isClosed()) {
                ClientProtocol namenode = this.getNameNode(connection);
                DatanodeStorageReport[] storageReport = this.getDiskReport(namenode);
                log.debug("There are {} DataNodes whose DiskReport will be analysed", storageReport.length);
                for(DatanodeStorageReport datanodeStorageReport: storageReport) {
                    if (targetHostAndRegion.containsKey(datanodeStorageReport.getDatanodeInfo().getHostName())){
                        if(datanodeStorageReport.getDatanodeInfo().getDfsUsedPercent() < DISK_USAGE_THRESHOLD_PERCENT) {
                            log.debug("Including {} as Eligible , DiskUsage : {}",datanodeStorageReport.getDatanodeInfo().getHostName(), datanodeStorageReport.getDatanodeInfo().getDfsUsedPercent() );
                            Set<RegionInfo> eligibleRegions = targetHostAndRegion.get(datanodeStorageReport.getDatanodeInfo().getHostName());
                            eligibleRegions.forEach( eligibleRegion -> report.put(eligibleRegion.getEncodedName(),
                                    new Pair<>(eligibleRegion, RegionEligibilityStatus.GREEN)));
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new CompactionRuntimeException(e);
        }
        log.info("{} regions present in final report prepared by {} without Filter", report.size(), report.getPreparedBy());
        return report;
    }

    @Override
    public Report getReport(CompactionContext context, HDFSConnection hdfsConnection, Report lastKnownReport) throws CompactionRuntimeException {
        Report overAllReport = this.getReport(context, hdfsConnection);
        Report finalreport = (Report)lastKnownReport.clone();
        lastKnownReport.keySet().forEach( encodedRegion ->  {
            if(!overAllReport.containsKey(encodedRegion)) {
                finalreport.remove(encodedRegion);
            }
        });
        log.info("{} regions present in final report prepared by {}", finalreport.size(), overAllReport.getPreparedBy());
        return finalreport;
    }


    @Override
    public void release(HDFSConnection hdfsConnection) {
        if(hdfsConnection.getHdfsConnection() != null) {
            try {
                hdfsConnection.getConnection().close();
            } catch (IOException e) {
                log.error("Could not close HBASE connection to cluster: {}", e.getMessage());
            }
        }

    }
}
