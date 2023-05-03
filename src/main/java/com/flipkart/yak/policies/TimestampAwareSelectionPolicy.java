package com.flipkart.yak.policies;

import com.flipkart.yak.config.CompactionContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Simple implementation of {@link com.flipkart.yak.interfaces.RegionSelectionPolicy} that ignores a region as eligible
 * candidate for compaction if compacted recently. Defaults to 1 day.
 */
@Slf4j
public class TimestampAwareSelectionPolicy extends NaiveRegionSelectionPolicy {

    private static long DELAY_BETWEEN_TWO_COMPACTIONS = 86400000;
    private static String KEY_DELAY_BETWEEN_TWO_COMPACTIONS = "compactor.policy.compaction.delay";

    @Override
    List<String> getEligibleRegions(Map<String, List<String>> regionFNHostnameMapping, Set<String> compactingRegions, List<RegionInfo> allRegions, Connection connection) throws IOException {
        List<String> regionsWhichCanBeCompacted = new ArrayList<>();
        Admin admin = connection.getAdmin();
        long currentTimestamp = EnvironmentEdgeManager.currentTime();
        for(RegionInfo region: allRegions) {
            long timestampMajorCompaction = admin.getLastMajorCompactionTimestampForRegion(region.getRegionName());
            log.debug("Region {} last compacted at {}", region, timestampMajorCompaction);
            if ((currentTimestamp - timestampMajorCompaction > DELAY_BETWEEN_TWO_COMPACTIONS) && !compactingRegions.contains(region)) {
                regionsWhichCanBeCompacted.add(region.getEncodedName());
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
