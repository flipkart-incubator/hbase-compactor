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
import java.util.*;

/**
 * Simple implementation of {@link com.flipkart.yak.interfaces.RegionSelectionPolicy} that ignores a region as eligible
 * candidate for compaction if compacted recently. Defaults to 1 day.
 */
@Slf4j
public class TimestampAwareSelectionPolicy extends NaiveRegionSelectionPolicy {

    private long DELAY_BETWEEN_TWO_COMPACTIONS = 86400000;
    private static String KEY_DELAY_BETWEEN_TWO_COMPACTIONS = "compactor.policy.compaction.delay";

    @Override
    List<String> getEligibleRegions(Map<String, List<String>> regionFNHostnameMapping, Set<String> compactingRegions, List<RegionInfo> allRegions, Connection connection) throws IOException {
        List<String> regionsWhichCanBeCompacted = new ArrayList<>();
        List<Pair<RegionInfo,Long>> sortedListOfRegionOnMCTime = new ArrayList<>();
        Admin admin = connection.getAdmin();
        long currentTimestamp = EnvironmentEdgeManager.currentTime();
        for(RegionInfo region: allRegions) {
            long timestampMajorCompaction = admin.getLastMajorCompactionTimestampForRegion(region.getRegionName());
            if (timestampMajorCompaction > 0) {
                sortedListOfRegionOnMCTime.add(new Pair<>(region, timestampMajorCompaction));
            }
        }
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
