package service;

import config.CompactorConfig;
import static config.CompactorConfig.FORCE_COMPACTION;
import static config.CompactorConfig.RECOMPACT_REGION_HOURS;
import static config.CompactorConfig.TABLE_NAME_KEY;
import static config.CompactorConfig.WAIT_TIME_KEY;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class CompactionService {
  private static Logger log = Logger.getLogger(CompactionService.class);

  private Admin admin;
  private Connection connection;
  private CompactorConfig compactorConfig;
  private int waitTime;
  private TableName tableName;
  private Map<String, RegionInfo> regionInfoMap = new HashMap<>();
  private final int maxFailuresAllowed = 20;

  public CompactionService(Connection connection, CompactorConfig config) throws IOException {
    this.tableName = TableName.valueOf((String) config.getConfig(TABLE_NAME_KEY));
    this.waitTime = ((int) config.getConfig(WAIT_TIME_KEY)) * 1000;
    this.admin = connection.getAdmin();
    this.compactorConfig = config;
    this.connection = connection;
    updateAllRegions();
  }

  private long getTimeAddHours(int numHours) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(new Date());
    cal.add(Calendar.HOUR, numHours);
    return cal.getTime().getTime();
  }

  private void updateAllRegions() throws IOException {
    List<RegionInfo> regionInfos = this.admin.getRegions(tableName);
    regionInfos.stream().forEach(regionInfo -> {
      regionInfoMap.putIfAbsent(regionInfo.getEncodedName(), regionInfo);
    });
  }

  public void start() throws IOException, InterruptedException {
    RegionFetcher regionFetcher = new RegionFetcher(connection, compactorConfig);
    log.info("Starting major compaction for table " + tableName);

    Set<String> compactingRegions = new HashSet<>();
    Set<String> completedRegions = new HashSet<>();
    int iteration = 0;
    int remainingFailures = Math.max(maxFailuresAllowed, regionInfoMap.size()); //One run can fail these many times
    while (true) {
      try {
        iteration += 1;
        updateAllRegions();
        log.info("Running iteration: " + iteration);
        for (String compactingRegion : compactingRegions) {
          CompactionState compactionState = admin.getCompactionStateForRegion(Bytes.toBytes(compactingRegion));
          log.info("Compaction state: " + compactingRegion + " - " + compactionState);
          if (compactionState.equals(CompactionState.MAJOR) || compactionState
              .equals(CompactionState.MAJOR_AND_MINOR)) {
            log.info("In Progress compaction for: " + compactingRegion);
          } else {
            log.info("Compaction complete for: " + compactingRegion);
            completedRegions.add(compactingRegion);
          }

        }
        compactingRegions.removeAll(completedRegions);

        List<String> regionsBatch = regionFetcher.getNextBatchOfEncodedRegions(compactingRegions);
        log.info("Received regions in this batch of size: " + regionsBatch.size() + " Regions: " + regionsBatch);

        if (regionsBatch.size() <= 0 && compactingRegions.size() <= 0) {
          break; // exhausted all regions
        }
        for (String encodedRegion : regionsBatch) {
          log.info("Starting compaction for: " + encodedRegion);
          if (completedRegions.contains(encodedRegion) || compactingRegions.contains(encodedRegion)) {
            log.info("Already completed or in progress: " + encodedRegion);
            continue;
          }
          if (regionInfoMap.containsKey(encodedRegion)) {
            long lastCompactionTime =
                admin.getLastMajorCompactionTimestampForRegion(regionInfoMap.get(encodedRegion).getRegionName());
            if (!(boolean) compactorConfig.getConfig(FORCE_COMPACTION)
                && getTimeAddHours(-1 * RECOMPACT_REGION_HOURS) < lastCompactionTime) {
              log.info(
                  "Skipping compaction for " + encodedRegion + ". Already compacted at " + new Date(lastCompactionTime)
                      + " which is later than " + new Date(getTimeAddHours(-1 * RECOMPACT_REGION_HOURS)));
              continue;
            }
          }
          admin.majorCompactRegion(encodedRegion.getBytes());
          log.info("Triggered compaction for: " + encodedRegion);
          compactingRegions.add(encodedRegion);
        }
      } catch (Exception ex) {
        log.error("Failed while running iteration: " + iteration + " remaining failures: " + remainingFailures
            + " with error: " + ex.getMessage(), ex);
        remainingFailures -= 1;
        if (remainingFailures <= 0) {
          log.warn("Aborting compaction run with some failures");
          break;
        }
      }
      log.info("Sleeping for " + this.waitTime);
      Thread.sleep(this.waitTime);
    }
    log.info("compaction complete for regions: " + completedRegions + " in-progress for regions: " + compactingRegions);
  }
}
