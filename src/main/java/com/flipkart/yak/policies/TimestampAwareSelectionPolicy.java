package com.flipkart.yak.policies;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class TimestampAwareSelectionPolicy extends BasePolicy {

    private static long DELAY_BETWEEN_TWO_COMPACTIONS = 86400000;
    private static String KEY_DELAY_BETWEEN_TWO_COMPACTIONS = "compactor.policy.compaction.delay";

    @Override
    List<String> getEligibleRegions(Map<String, List<String>> regionFNHostnameMapping, Set<String> compactingRegions, List<RegionInfo> allRegions) throws IOException {
        List<String> regionsWhichCanBeCompacted = new ArrayList<>();
        long currentTimestamp = EnvironmentEdgeManager.currentTime();
        for(RegionInfo region: allRegions) {
            long timestampMajorCompaction = this.admin.getLastMajorCompactionTimestampForRegion(region.getRegionName());
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
