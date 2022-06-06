package service;

import config.CompactorConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import static config.CompactorConfig.*;

public class RegionFetcher {

  private static Logger log = Logger.getLogger(RegionFetcher.class);

  private Admin admin;
  private Table metaTable;
  private int maxConcurrentRegions;
  private TableName tableName;
  private final static String HBASE_NS = "hbase";
  private final static String META_TABLE = "meta";
  private final int MAX_PARALLEL_SERVER_COMPACTION;
  private final static byte[] INFO_CF = Bytes.toBytes("info");
  private final static byte[] FN_CQ = Bytes.toBytes("fn");
  private Map<String, List<String>> regionFNHostnameMapping = new HashMap<>();
  private Set<String> processedRegions = new HashSet<>();
  private Set<String> allRegions = new HashSet<>();
  private int iteration = 0;

  public RegionFetcher(Connection connection, CompactorConfig config) throws IOException {
    this.metaTable = connection.getTable(TableName.valueOf(HBASE_NS, META_TABLE));
    this.maxConcurrentRegions = (int) config.getConfig(BATCH_SIZE_KEY);
    this.tableName = TableName.valueOf((String) config.getConfig(TABLE_NAME_KEY));
    this.MAX_PARALLEL_SERVER_COMPACTION = (int)config.getConfig(SERVER_PARALLEL_TASKS);
    this.admin = connection.getAdmin();
    log.info("Max Concurrent Region: "+ this.maxConcurrentRegions);
    log.info("Max Parallel Compaction Task: "+ this.MAX_PARALLEL_SERVER_COMPACTION);
    log.info("Table Name: "+ this.tableName);
    this.refreshFavoredNodesMapping();
    this.refreshRegions();
  }

  private List<String> getFavoredNodesList(byte[] favoredNodes) throws IOException {
    HBaseProtos.FavoredNodes f = HBaseProtos.FavoredNodes.parseFrom(favoredNodes);
    List<HBaseProtos.ServerName> protoNodes = f.getFavoredNodeList();
    ServerName[] servers = new ServerName[protoNodes.size()];
    int i = 0;
    for (HBaseProtos.ServerName node : protoNodes) {
      servers[i++] = ProtobufUtil.toServerName(node);
    }
    return Arrays.asList(servers).stream().map(server -> server.getHostname()).collect(Collectors.toList());
  }

  private void refreshFavoredNodesMapping() throws IOException {
    Optional<PrefixFilter> filterOptional = Optional.of(new PrefixFilter(Bytes.toBytes(tableName + ",")));
    ResultScanner scanner = metaTable.getScanner(getScan(INFO_CF, FN_CQ, filterOptional));

    while (true) {
      Result[] results = scanner.next(10);
      if (results != null && results.length > 0) {
        for (int index = 0; index < results.length; index += 1) {
          Result result = results[index];
          List<String> fnHostsList = getFavoredNodesList(result.getValue(INFO_CF, FN_CQ));
          String[] tokens = Bytes.toString(result.getRow()).split("\\.");
          log.trace("Identified Region: " + tokens[tokens.length - 1] + " fns: " + fnHostsList);
          if (fnHostsList.size() > 0) {
            regionFNHostnameMapping.put(tokens[tokens.length - 1], fnHostsList);
          } else {
            regionFNHostnameMapping.putIfAbsent(tokens[tokens.length - 1], fnHostsList);
          }
        }
      } else {
        break;
      }
    }
  }

  private void refreshRegions() throws IOException {
    allRegions.clear();
    allRegions.addAll(
        this.admin.getRegions(tableName).stream().map(region -> region.getEncodedName()).collect(Collectors.toList()));

    for (ServerName sn : this.admin.getRegionServers()) {
      List<RegionInfo> regions = this.admin.getRegions(sn);
      regions.stream().forEach(region -> {
        if (allRegions.contains(region.getEncodedName())) {
          ArrayList<String> temp = new ArrayList<String>();
          temp.add(sn.getHostname());
          regionFNHostnameMapping.putIfAbsent(region.getEncodedName(), temp);
        }
      });
    }
    log.trace("All regions: " + allRegions);
  }

  private Scan getScan(byte[] cf, byte[] column, Optional<PrefixFilter> filterOptional) {
    Scan scan = new Scan();
    scan.addColumn(cf, column);
    if (filterOptional.isPresent()) {
      scan.setFilter(filterOptional.get());
    }
    return scan;
  }

  public List<String> getNextBatchOfEncodedRegions(Set<String> inProgressRegions) throws IOException {
    iteration += 1;
    log.info("Fetching regions for this iteration: " + iteration + ", regionsCompactionInProgress: " + inProgressRegions
        .size() + ", regionsCompacted: " + (processedRegions.size() - inProgressRegions.size()) + ", allRegions: "
        + allRegions.size());
    List<String> encodedRegions = new ArrayList<>();
    refreshRegions();
    refreshFavoredNodesMapping();

    Map<String, Integer> serversForThisBatch = new HashMap<>();
    for (String encodedRegion : inProgressRegions) {
      if (regionFNHostnameMapping.containsKey(encodedRegion)) {
        this.updateParallelCompactionCount(encodedRegion, serversForThisBatch);
      }
    }

    for (String encodedRegion : allRegions) {
      if (!inProgressRegions.contains(encodedRegion) && encodedRegions.size() < (
          maxConcurrentRegions - inProgressRegions.size()) && !processedRegions.contains(encodedRegion)) {
        if (!regionFNHostnameMapping.containsKey(encodedRegion)) {
          throw new IOException("Invalid favored nodes for region: " + encodedRegion);
        }
        boolean shouldAdd = true;
        for (String fn : regionFNHostnameMapping.get(encodedRegion)) {
          if (serversForThisBatch.containsKey(fn) && serversForThisBatch.get(fn) > this.MAX_PARALLEL_SERVER_COMPACTION) {
            shouldAdd = false;
            break;
          }
        }
        if (shouldAdd) {
          log.info("Adding server " + regionFNHostnameMapping.get(encodedRegion) + " for Region: " + encodedRegion);
          this.updateParallelCompactionCount(encodedRegion, serversForThisBatch);
          encodedRegions.add(encodedRegion);
        }
      }
    }
    log.info("Returning servers: " + serversForThisBatch + ", and regions: " + encodedRegions);
    processedRegions.addAll(encodedRegions);
    return encodedRegions;
  }

  private void updateParallelCompactionCount(String encodedRegion, Map<String, Integer> compactingServersTaskCount) {
    for(String s: regionFNHostnameMapping.get(encodedRegion)) {
      compactingServersTaskCount.putIfAbsent(s,0);
      compactingServersTaskCount.compute(s, (k,v) -> v==null ? 1: v+1);
    }
  }
}
