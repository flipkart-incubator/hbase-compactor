package com.flipkart.yak.policies;

import com.flipkart.yak.commons.HBaseUtils;
import com.flipkart.yak.commons.RegionEligibilityStatus;
import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.core.CompactionRuntimeException;
import com.flipkart.yak.core.MonitorService;
import com.flipkart.yak.core.CompactionMetricsService;
import com.flipkart.yak.interfaces.RegionSelectionPolicy;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Base implementation of {@link RegionSelectionPolicy} which uses {@link Connection} to get primitive information
 * like list of Regions, state of compaction etc. Algorithms to select Regions which are solely dependent on HBASE cluster
 * info must extend this class. Not useful for algorithms which does not need HBASE cluster info heavily e.g
 * {@link HadoopDiskUsageBasedSelectionPolicy}
 */
@Slf4j
public abstract class HBASEBasePolicy implements RegionSelectionPolicy<Connection> {

    @Override
    public Report getReport(CompactionContext context, Connection connection) throws CompactionRuntimeException {
        try {
            Admin admin = connection.getAdmin();
            List<RegionInfo> allRegionInfoForThis = HBaseUtils.getRegionsAll(context, admin);
            MonitorService.updateHistogram(this.getClass(), context, "totalRegions", allRegionInfoForThis.size());
            CompactionMetricsService.updateMetrics(context, connection, allRegionInfoForThis);
            Report tempReport = new Report(this.getClass().getName());
            allRegionInfoForThis.forEach(a -> {
                tempReport.put(a.getEncodedName(), new Pair<>(a, RegionEligibilityStatus.GREEN));
            });
            return this.getReport(context, connection, tempReport);
        } catch (IOException e) {
            log.error("Exception while getting eligibility report {}", e.getMessage());
            throw new CompactionRuntimeException(e);
        }
    }

    @Override
    public Report getReport(CompactionContext context, Connection connection, Report report) throws CompactionRuntimeException {
        try {
            Admin admin = connection.getAdmin();
            List<RegionInfo> allRegions = report.values().stream().map(Pair::getFirst).collect(Collectors.toList());
            List<String> allEncodedRegionName = allRegions.stream().map(RegionInfo::getEncodedName).collect(Collectors.toList());
            Set<String> compactingRegions = this.getCompactingRegion(allEncodedRegionName, admin);
            Map<String, List<String>> regionFNMapping = new WeakHashMap<>();
            HBaseUtils.refreshRegionToNodeMapping(admin, allEncodedRegionName, regionFNMapping);
            List<String> eligibleRegions = this.getEligibleRegions(regionFNMapping, compactingRegions, allRegions, connection);
            return this.prepareReport(allRegions, eligibleRegions);
        }catch (IOException e) {
            log.error("Exception while getting eligibility report {}", e.getMessage());
            throw new CompactionRuntimeException(e);
        }
    }


    private Set<String> getCompactingRegion(List<String> regions, Admin admin) throws IOException {
        Set<String> compactingRegions = new HashSet<>();
        for (String region : regions) {
            CompactionState compactionState = admin.getCompactionStateForRegion(Bytes.toBytes(region));
            log.trace("Compaction state: " + region + " - " + compactionState);
            if (compactionState.equals(CompactionState.MAJOR) || compactionState
                    .equals(CompactionState.MAJOR_AND_MINOR)) {
                log.debug("In Progress compaction for: " + region);
                compactingRegions.add(region);
            }
        }
        log.info("Found {} compacting regions ", compactingRegions.size());
        return compactingRegions;
    }

    private Report prepareReport(List<RegionInfo> allRegionInfo, List<String> eligibleRegions) {
        Report finalReport = new Report(this.getClass().getName());
        Map<String, RegionInfo> tempStore = new WeakHashMap<>();
        allRegionInfo.forEach(R -> tempStore.put(R.getEncodedName(), R));
        eligibleRegions.forEach(region -> finalReport.put(region, new Pair<>(tempStore.get(region), RegionEligibilityStatus.GREEN)));
        log.info("{} regions present in final report prepared by {}", finalReport.size(), this.getClass().getName());
        return finalReport;
    }


    abstract List<String> getEligibleRegions(Map<String, List<String>> regionFNHostnameMapping,
                                                     Set<String> compactingRegions,
                                             List<RegionInfo> allRegions,
                                             Connection connection) throws IOException;
}
