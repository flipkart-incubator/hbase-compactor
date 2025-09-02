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
    private long MIN_DAYS_ALLOWED_BETWEEN_CONSECUTIVE_COMPACTIONS_OF_REGION = TimeUnit.DAYS.toMillis(3);
    private static String KEY_MIN_DAYS_ALLOWED_BETWEEN_CONSECUTIVE_COMPACTIONS_OF_REGION = "compactor.policy.min.days.between.consecutive.compactions";


    @Override
    List<String> getEligibleRegions(Map<String, List<String>> regionFNHostnameMapping, Set<String> compactingRegions, List<RegionInfo> allRegions, Connection connection, CompactionContext context) throws IOException {
        List<String> regionsWhichCanBeCompacted = new ArrayList<>();
        List<Pair<RegionInfo,Long>> sortedListOfRegionOnMCTime = new ArrayList<>();
        Admin admin = connection.getAdmin();
        long currentTimestamp = EnvironmentEdgeManager.currentTime();
        int regionsNotCompactedIn3Days = 0, regionsCompactedEarlierThan3Days = 0;
        for(RegionInfo region: allRegions) {
            try {
                long timestampMajorCompaction = admin.getLastMajorCompactionTimestampForRegion(region.getRegionName());
                if (timestampMajorCompaction > 0) {
                    sortedListOfRegionOnMCTime.add(new Pair<>(region, timestampMajorCompaction));
                    long timeSinceLastCompaction = currentTimestamp - timestampMajorCompaction;
                    if (timeSinceLastCompaction > MIN_DAYS_ALLOWED_BETWEEN_CONSECUTIVE_COMPACTIONS_OF_REGION) {
                        regionsNotCompactedIn3Days++;
                        regionsCompactedEarlierThan3Days++;
                        log.info("Region {} not compacted in last 3 days (last compacted: {})",
                                region.getEncodedName(),
                                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestampMajorCompaction)));
                    }
                } else {
                    regionsNotCompactedIn3Days++;
                }
            } catch (Exception e) {
                regionsNotCompactedIn3Days++;
                log.warn("Failed to get compaction timestamp for region {}: {}", region.getEncodedName(), e.getMessage());
            }
        }
        MonitorService.setCounterValue(this.getClass(), context, "regionsNotCompactedIn3Days", regionsNotCompactedIn3Days);
        MonitorService.setCounterValue(this.getClass(), context, "regionsCompactedEarlierThan3Days", regionsCompactedEarlierThan3Days);

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
                if (pair.getFirst().equals(KEY_MIN_DAYS_ALLOWED_BETWEEN_CONSECUTIVE_COMPACTIONS_OF_REGION)) {
                    MIN_DAYS_ALLOWED_BETWEEN_CONSECUTIVE_COMPACTIONS_OF_REGION = TimeUnit.DAYS.toMillis(Long.parseLong(pair.getSecond()));
                }
            });
        }
        log.info("Delay between two compactions: {}", DELAY_BETWEEN_TWO_COMPACTIONS);
        log.info("Monitoring threshold for regions not compacted: {} days", TimeUnit.MILLISECONDS.toDays(MIN_DAYS_ALLOWED_BETWEEN_CONSECUTIVE_COMPACTIONS_OF_REGION));
    }
}
