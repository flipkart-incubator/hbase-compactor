package com.flipkart.yak.policies;

import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.core.MonitorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
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
        if (allRegions == null || allRegions.isEmpty()) {
            log.warn("No regions provided for eligibility check");
            return Collections.emptyList();
        }

        long currentTimestamp = EnvironmentEdgeManager.currentTime();
        int regionsNotCompacted = 0;

        Map<String, RegionCompactionInfo> regionInfoMap;
        try (Admin admin = connection.getAdmin()) {
            regionInfoMap = buildRegionCompactionInfo(admin, allRegions);
        }

        List<Pair<RegionInfo, Long>> sortedListOfRegionOnMCTime = new ArrayList<>(allRegions.size());
        List<String> regionsWhichCanBeCompacted = new ArrayList<>();

        for (RegionInfo region : allRegions) {
            String encodedName = region.getEncodedName();
            RegionCompactionInfo info = regionInfoMap.getOrDefault(encodedName, RegionCompactionInfo.UNKNOWN);
            long timestampMajorCompaction = info.lastMajorCompactionTs;
            int storeFileCount = info.storeFileCount;
            long timeSinceLastCompaction = currentTimestamp - timestampMajorCompaction;

            sortedListOfRegionOnMCTime.add(new Pair<>(region, timestampMajorCompaction));

            if (timestampMajorCompaction > 0) {
                if (timeSinceLastCompaction > MIN_DAYS_ALLOWED_BETWEEN_CONSECUTIVE_COMPACTIONS_OF_REGION) {
                    regionsNotCompacted++;
                }
            } else {
                regionsNotCompacted++;
            }

            boolean neverCompactedButHasData = (timestampMajorCompaction == 0) && (storeFileCount > 0);
            boolean dueForCompaction = (timestampMajorCompaction > 0)
                    && (timeSinceLastCompaction > DELAY_BETWEEN_TWO_COMPACTIONS);
            if (neverCompactedButHasData || dueForCompaction) {
                regionsWhichCanBeCompacted.add(encodedName);
            }
        }

        MonitorService.updateGauge(this.getClass(), context, "regionsNotCompacted", regionsNotCompacted);

        sortedListOfRegionOnMCTime.sort(Comparator.comparing(Pair::getSecond));
        int size = sortedListOfRegionOnMCTime.size();
        if (size > 0) {
            long oldestTs = sortedListOfRegionOnMCTime.get(0).getSecond();
            long newestTs = sortedListOfRegionOnMCTime.get(size - 1).getSecond();
            log.info("Compaction timestamp range: oldest={}, newest={}", oldestTs, newestTs);
        }

        log.info("Marked {} of {} regions as eligible for compaction ({} already compacting, {} overdue)",
                regionsWhichCanBeCompacted.size(), allRegions.size(),
                compactingRegions.size(), regionsNotCompacted);

        return regionsWhichCanBeCompacted;
    }

    private Map<String, RegionCompactionInfo> buildRegionCompactionInfo(Admin admin, List<RegionInfo> allRegions) {
        Set<String> targetRegions = new HashSet<>(allRegions.size());
        for (RegionInfo ri : allRegions) {
            targetRegions.add(ri.getEncodedName());
        }

        Map<String, RegionCompactionInfo> result = new HashMap<>(allRegions.size());
        try {
            ClusterMetrics clusterMetrics = admin.getClusterMetrics(EnumSet.of(ClusterMetrics.Option.LIVE_SERVERS));
            for (ServerMetrics serverMetrics : clusterMetrics.getLiveServerMetrics().values()) {
                for (RegionMetrics regionMetrics : serverMetrics.getRegionMetrics().values()) {
                    String encodedName = RegionInfo.encodeRegionName(regionMetrics.getRegionName());
                    if (targetRegions.contains(encodedName)) {
                        result.put(encodedName, new RegionCompactionInfo(
                                regionMetrics.getStoreFileCount(),
                                regionMetrics.getLastMajorCompactionTimestamp()));
                    }
                }
            }
        } catch (IOException e) {
            log.error("Failed to fetch cluster metrics for region compaction info", e);
        }
        return result;
    }

    private static class RegionCompactionInfo {
        static final RegionCompactionInfo UNKNOWN = new RegionCompactionInfo(0, 0);

        final int storeFileCount;
        final long lastMajorCompactionTs;

        RegionCompactionInfo(int storeFileCount, long lastMajorCompactionTs) {
            this.storeFileCount = storeFileCount;
            this.lastMajorCompactionTs = lastMajorCompactionTs;
        }
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
