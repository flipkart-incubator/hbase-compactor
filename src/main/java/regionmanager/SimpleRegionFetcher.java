package regionmanager;

import config.CompactorConfig;
import static config.CompactorConfig.BATCH_SIZE_KEY;
import static config.CompactorConfig.TABLE_NAME_KEY;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.log4j.Logger;
import utils.MetaInfoUtils;

public class SimpleRegionFetcher implements  IRegionFetcher {

  private static Logger log = Logger.getLogger(SimpleRegionFetcher.class);
  private Admin admin;
  private int maxConcurrentRegions;
  private TableName tableName;
  private Connection connection;

  private Map<String, List<String>> regionFNHostnameMapping = new HashMap<>();
  private Set<String> processedRegions = new HashSet<>();
  private Set<String> allRegions = new HashSet<>();
  private int iteration = 0;

  protected SimpleRegionFetcher(Connection connection, CompactorConfig config) throws IOException {
    this.maxConcurrentRegions = (int) config.getConfig(BATCH_SIZE_KEY);
    this.tableName = TableName.valueOf((String) config.getConfig(TABLE_NAME_KEY));
    this.admin = connection.getAdmin();
    this.connection = connection;
    MetaInfoUtils.getInstance(connection).refreshFavoredNodesMapping(this.tableName, this.regionFNHostnameMapping);
    this.refreshRegions();
  }

  private void refreshRegions() throws IOException {
    allRegions.clear();
    allRegions.addAll(this.admin.getRegions(tableName).stream().map(region -> region.getEncodedName()).collect(Collectors.toList()));

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

  @Override
  public List<String> getNextBatchOfEncodedRegions(Set<String> inProgressRegions) throws IOException {
    iteration += 1;
    log.info("Fetching regions for this iteration: " + iteration + ", regionsCompactionInProgress: " + inProgressRegions
        .size() + ", regionsCompacted: " + (processedRegions.size() - inProgressRegions.size()) + ", allRegions: "
        + allRegions.size());
    List<String> encodedRegions = new ArrayList<>();
    refreshRegions();
    MetaInfoUtils.getInstance(connection).refreshFavoredNodesMapping(this.tableName, this.regionFNHostnameMapping);

    Set<String> serversForThisBatch = new HashSet<>();
    for (String encodedRegion : inProgressRegions) {
      if (regionFNHostnameMapping.containsKey(encodedRegion)) {
        serversForThisBatch.addAll(regionFNHostnameMapping.get(encodedRegion));
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
          if (serversForThisBatch.contains(fn)) {
            shouldAdd = false;
            break;
          }
        }
        if (shouldAdd) {
          serversForThisBatch.addAll(regionFNHostnameMapping.get(encodedRegion));
          encodedRegions.add(encodedRegion);
        }
      }
    }
    log.info("Returning servers: " + serversForThisBatch + ", and regions: " + encodedRegions);
    processedRegions.addAll(encodedRegions);
    return encodedRegions;
  }
}
