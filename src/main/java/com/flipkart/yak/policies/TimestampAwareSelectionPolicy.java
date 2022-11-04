package com.flipkart.yak.policies;

import lombok.extern.slf4j.Slf4j;
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
    List<String> getEligibleRegions(Map<String, List<String>> regionFNHostnameMapping, Set<String> compactingRegions, List<String> allRegions) throws IOException {
        List<String> regionsWhichCanBeCompacted = new ArrayList<>();
        long currentTimestamp = EnvironmentEdgeManager.currentTime();
        for(String region: allRegions) {
            long timestampMajorCompaction = this.admin.getLastMajorCompactionTimestampForRegion(Bytes.toBytes(region));
            log.debug("Region {} last compacted at {}", region, timestampMajorCompaction);
            if (currentTimestamp - timestampMajorCompaction > DELAY_BETWEEN_TWO_COMPACTIONS) {
                regionsWhichCanBeCompacted.add(region);
            }
        }
        log.info("marked {} regions as eligible region ", regionsWhichCanBeCompacted.size());
        return regionsWhichCanBeCompacted;
    }

    @Override
    public void setFromConfig(List<Pair<String, String>> configs) {
        configs.forEach(pair -> {
            if(pair.getFirst().equals(KEY_DELAY_BETWEEN_TWO_COMPACTIONS)) {
                DELAY_BETWEEN_TWO_COMPACTIONS = Long.parseLong(pair.getSecond());
            }
        });
        log.info("Delay between two compactions: {}", DELAY_BETWEEN_TWO_COMPACTIONS);
    }
}
