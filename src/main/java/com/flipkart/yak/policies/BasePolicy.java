package com.flipkart.yak.policies;

import com.flipkart.yak.commons.RegionEligibilityStatus;
import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.core.CompactionRuntimeException;
import com.flipkart.yak.interfaces.RegionSelectionPolicy;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


@Slf4j
public abstract class BasePolicy implements RegionSelectionPolicy {

    protected Admin admin;

    @Override
    public Report getReport(CompactionContext context, Connection connection) throws CompactionRuntimeException {
        try {
            admin = connection.getAdmin();
            TableName tableName= TableName.valueOf(context.getTableName());
            List<RegionInfo> allRegionInfoForThis = this.getAllRegions(tableName,admin);
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
            List<String> allRegions = new ArrayList<>(report.keySet());
            Set<String> compactingRegions = this.getCompactingRegion(allRegions, admin);
            Map<String, List<String>> regionFNMapping = new WeakHashMap<>();
            this.refreshRegionToHostNameMapping(admin, allRegions, regionFNMapping);
            List<String> eligibleRegions = this.getEligibleRegions(regionFNMapping, compactingRegions, allRegions);
            return this.prepareReport(report.entrySet().stream().map(e -> e.getValue().getFirst()).collect(Collectors.toList()), eligibleRegions);
        }catch (IOException e) {
            log.error("Exception while getting eligibility report {}", e.getMessage());
            throw new CompactionRuntimeException(e);
        }
    }


    private Set<String> getCompactingRegion(List<String> regions, Admin admin) throws IOException {
        Set<String> compactingRegions = new HashSet<>();
        for (String region : regions) {
            CompactionState compactionState = admin.getCompactionStateForRegion(Bytes.toBytes(region));
            log.debug("Compaction state: " + region + " - " + compactionState);
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
        Set<String> setOfRegionKeys = new HashSet<>(eligibleRegions);
        allRegionInfo.forEach(region -> {
            if (setOfRegionKeys.contains(region.getEncodedName())) {
                finalReport.put(region.getEncodedName(), new Pair<>(region, RegionEligibilityStatus.GREEN));
            }
        });
        log.info("{} regions present in final report prepared by {}", finalReport.size(), this.getClass().getName());
        return finalReport;
    }

    private List<RegionInfo> getAllRegions(TableName tableName, Admin admin) throws IOException {
        List<RegionInfo> allRegions = admin.getRegions(tableName);
        log.debug("found {} regions for table {}", allRegions.size(), tableName);
        return allRegions;
    }

    private void refreshRegionToHostNameMapping(Admin admin, List<String> allRegions,
                                                Map<String, List<String>> regionFNHostnameMapping)
            throws IOException {
        for (ServerName sn : admin.getRegionServers()) {
            List<RegionInfo> regions = admin.getRegions(sn);
            regions.forEach(region -> {
                if (allRegions.contains(region.getEncodedName())) {
                    regionFNHostnameMapping.putIfAbsent(region.getEncodedName(), new ArrayList<>());
                    regionFNHostnameMapping.get(region.getEncodedName()).add(sn.getHostname());
                    log.debug("adding region {} for Favoured Node {}", region.getEncodedName(), sn.getHostname());
                }
            });
        }
        log.info("there are {} keys present in region-To-Favoured-Node mapping, " +
                "ideally it should be total number of regions", regionFNHostnameMapping.size());
    }


    abstract List<String> getEligibleRegions(Map<String, List<String>> regionFNHostnameMapping,
                                                     Set<String> compactingRegions, List<String> allRegions) throws IOException;
}
