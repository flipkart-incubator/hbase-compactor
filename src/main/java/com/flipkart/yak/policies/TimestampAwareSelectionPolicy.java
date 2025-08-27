package com.flipkart.yak.policies;

import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.core.MonitorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Simple implementation of {@link com.flipkart.yak.interfaces.RegionSelectionPolicy} that ignores a region as eligible
 * candidate for compaction if compacted recently. Defaults to 1 day.
 */
@Slf4j
public class TimestampAwareSelectionPolicy extends NaiveRegionSelectionPolicy {

    private long DELAY_BETWEEN_TWO_COMPACTIONS = 86400000;
    private static String KEY_DELAY_BETWEEN_TWO_COMPACTIONS = "compactor.policy.compaction.delay";

    @Override
    List<String> getEligibleRegions(Map<String, List<String>> regionFNHostnameMapping, Set<String> compactingRegions, List<RegionInfo> allRegions, Connection connection, CompactionContext context) throws IOException {
        List<String> regionsWhichCanBeCompacted = new ArrayList<>();
        List<Pair<RegionInfo,Long>> sortedListOfRegionOnMCTime = new ArrayList<>();
        Admin admin = connection.getAdmin();
        long currentTimestamp = EnvironmentEdgeManager.currentTime();

        final long THREE_DAYS_MILLIS = TimeUnit.DAYS.toMillis(3);
        int totalRegions = allRegions.size();
        int regionsNotCompactedIn3Days = 0;
        List<String> nonCompactedRegionsIn3Days = new ArrayList<>();
        Map<String, String> regionCompactionStatus = new HashMap<>();

        for(RegionInfo region: allRegions) {
            try {
                long timestampMajorCompaction = admin.getLastMajorCompactionTimestampForRegion(region.getRegionName());

                if (timestampMajorCompaction > 0) {
                    sortedListOfRegionOnMCTime.add(new Pair<>(region, timestampMajorCompaction));

                    long timeSinceLastCompaction = currentTimestamp - timestampMajorCompaction;
                    if (timeSinceLastCompaction > THREE_DAYS_MILLIS) {
                        regionsNotCompactedIn3Days++;
                        nonCompactedRegionsIn3Days.add(region.getEncodedName());

                        regionCompactionStatus.put(region.getEncodedName(),
                                String.format("%s (last compacted: %s, %d days ago)",
                                        region.getEncodedName(),
                                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestampMajorCompaction)),
                                        TimeUnit.MILLISECONDS.toDays(timeSinceLastCompaction)));
                    }
                } else {
                    regionsNotCompactedIn3Days++;
                    nonCompactedRegionsIn3Days.add(region.getEncodedName());
                    regionCompactionStatus.put(region.getEncodedName(),
                            String.format("%s (never compacted)",
                                    region.getEncodedName()));
                }
            } catch (Exception e) {
                regionsNotCompactedIn3Days++;
                nonCompactedRegionsIn3Days.add(region.getEncodedName());
                regionCompactionStatus.put(region.getEncodedName(),
                        String.format("%s (error getting timestamp: %s)",
                                region.getEncodedName(),
                                e.getMessage()));
                log.warn("Failed to get compaction timestamp for region {} in table {}: {}",
                        region.getEncodedName(), region.getTable().getNameAsString(), e.getMessage());
            }
        }

        double percentage = totalRegions > 0 ? (double) regionsNotCompactedIn3Days / totalRegions * 100.0 : 0.0;

        if (regionsNotCompactedIn3Days > 0) {
            log.warn("REGIONS NOT COMPACTED IN LAST THREE DAYS: Found {} out of {} regions ({}%)",
                    regionsNotCompactedIn3Days, totalRegions, String.format("%.1f", percentage));

            Map<String, List<String>> regionsByTable = new HashMap<>();
            for (String regionId : nonCompactedRegionsIn3Days) {
                String status = regionCompactionStatus.get(regionId);
                if (status != null) {
                    // Find the table name for this region from allRegions
                    String tableName = null;
                    for (RegionInfo region : allRegions) {
                        if (region.getEncodedName().equals(regionId)) {
                            tableName = region.getTable().getNameAsString();
                            break;
                        }
                    }

                    if (tableName != null) {
                        regionsByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(status);
                    }
                }
            }

            for (Map.Entry<String, List<String>> entry : regionsByTable.entrySet()) {
                String tableName = entry.getKey();
                List<String> tableRegions = entry.getValue();
                log.warn("   Table '{}' has {} problematic regions:", tableName, tableRegions.size());
                for (String regionStatus : tableRegions) {
                    log.warn("     - {}", regionStatus);
                }
            }
        } else {
            log.info("COMPACTION MONITORING: All {} regions are healthy (compacted within last three days)", totalRegions);
        }

        MonitorService.setCounterValue(this.getClass(), context, "regionsNotCompactedIn3Days", regionsNotCompactedIn3Days);
        sortedListOfRegionOnMCTime.sort(Comparator.comparing(Pair::getSecond));
        int size = sortedListOfRegionOnMCTime.size();
        if (size > 0) {
            long lowestTimeStamp = sortedListOfRegionOnMCTime.get(0).getSecond();
            long highestTimeStamp = sortedListOfRegionOnMCTime.get(size -1).getSecond();
            log.info("Region With Earliest timeStamp: {}, Region With Oldest timeStamp: {}", lowestTimeStamp, highestTimeStamp);
        }
        for(Pair<RegionInfo,Long> region: sortedListOfRegionOnMCTime) {
            log.info("Region {} last compacted at {}", region, region.getSecond());
            if ((currentTimestamp - region.getSecond() > DELAY_BETWEEN_TWO_COMPACTIONS) && !compactingRegions.contains(region)) {
                regionsWhichCanBeCompacted.add(region.getFirst().getEncodedName());
            }
        }
        log.info("marked {} regions as eligible region ", regionsWhichCanBeCompacted.size());
        return regionsWhichCanBeCompacted;
    }

    @Override
    public void setFromConfig(List<Pair<String, String>> configs) {
        if (configs!= null) {
            configs.forEach(pair -> {
                if (pair.getFirst().equals(KEY_DELAY_BETWEEN_TWO_COMPACTIONS)) {
                    DELAY_BETWEEN_TWO_COMPACTIONS = Long.parseLong(pair.getSecond());
                }
            });
        }
        log.info("Delay between two compactions: {}", DELAY_BETWEEN_TWO_COMPACTIONS);
    }
}