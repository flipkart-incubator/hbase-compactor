package com.flipkart.yak.policies;

import com.flipkart.yak.commons.RegionEligibilityStatus;
import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.core.CompactionRuntimeException;
import com.flipkart.yak.interfaces.RegionSelectionPolicy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class NaiveRegionSelectionPolicy implements RegionSelectionPolicy {


    private final static String HBASE_NS = "hbase";
    private final static String META_TABLE = "meta";
    private final static byte[] INFO_CF = Bytes.toBytes("info");
    private final static byte[] FN_CQ = Bytes.toBytes("fn");

    public static String KEY_MAX_PARALLEL_COMPACTION = "compactor.policy.max.parallel.compaction.per.server";
    public static String KEY_MAX_PARALLEL_COMPACTION_FOR_TABLE = "compactor.policy.max.parallel.compaction.per.table";
    private static int MAX_PARALLEL_COMPACTION_PER_SERVER = 1;
    private static int MAX_PARALLEL_COMPACTION_PER_TARGET = 5;

    @Override
    public void setFromConfig(List<Pair<String, String>> configs) {
        log.info("Max Parallel Compaction for a server allowed {}", MAX_PARALLEL_COMPACTION_PER_SERVER);
    }

    @Override
    public Report getReport(CompactionContext context, Connection connection) throws CompactionRuntimeException {
        try {
            Admin admin = connection.getAdmin();
            TableName tableName= TableName.valueOf(context.getTableName());
            List<RegionInfo> allRegionInfoList = this.getAllRegions(tableName,admin);
            List<String> allRegions = allRegionInfoList.stream().map(RegionInfo::getEncodedName).collect(Collectors.toList());
            Set<String> compactingRegions = this.getCompactingRegion(allRegions, admin);
            Map<String, List<String>> regionFNMapping = new WeakHashMap<>();
            this.refreshRegionToHostNameMapping(admin, tableName, allRegions, regionFNMapping);
            List<String> eligibleRegions = this.getEligibleRegions(regionFNMapping, compactingRegions, allRegions);
            return this.prepareReport(allRegionInfoList, eligibleRegions);
        } catch (IOException e) {
            log.error("Exception while getting eligibility report {}", e.getMessage());
            throw new CompactionRuntimeException(e);
        }
    }

    private Report prepareReport(List<RegionInfo> allRegionInfo, List<String> eligibleRegions) {
        Report finalReport = new Report();
        Set<String> setOfRegionKeys = new HashSet<>(eligibleRegions);
        allRegionInfo.forEach(region -> {
            if (setOfRegionKeys.contains(region.getEncodedName())) {
                finalReport.put(region.getEncodedName(), new Pair<>(region, RegionEligibilityStatus.GREEN));
            }
        });
        log.info("{} regions present in final report prepared by {}", finalReport.size(), this.getClass().getName());
        return finalReport;
    }

    private List<String> getFavoredNodesList(byte[] favoredNodes) throws IOException {
        HBaseProtos.FavoredNodes f = HBaseProtos.FavoredNodes.parseFrom(favoredNodes);
        List<HBaseProtos.ServerName> protoNodes = f.getFavoredNodeList();
        ServerName[] servers = new ServerName[protoNodes.size()];
        int i = 0;
        for (HBaseProtos.ServerName node : protoNodes) {
            servers[i++] = ProtobufUtil.toServerName(node);
        }
        return Arrays.stream(servers).map(ServerName::getHostname).collect(Collectors.toList());
    }

    private void refreshFavoredNodesMapping(Map<String, List<String>> regionFNHostnameMapping, Table metaTable, TableName tableName) throws IOException {
        Optional<PrefixFilter> filterOptional = Optional.of(new PrefixFilter(Bytes.toBytes(tableName + ",")));
        ResultScanner scanner = metaTable.getScanner(getScan(filterOptional));
        Result[] results;
        do {
            results = scanner.next(10);
            for (int index = 0; index < results.length; index++) {
                    Result result = results[index];
                    List<String> fnHostsList = getFavoredNodesList(result.getValue(INFO_CF, FN_CQ));
                    String[] tokens = Bytes.toString(result.getRow()).split("\\.");
                    log.debug("Identified Region: " + tokens[tokens.length - 1] + " fns: " + fnHostsList);
                    if (fnHostsList.size() > 0) {
                        regionFNHostnameMapping.put(tokens[tokens.length - 1], fnHostsList);
                    } else {
                        regionFNHostnameMapping.putIfAbsent(tokens[tokens.length - 1], fnHostsList);
                    }
            }
        } while (results.length > 0);
    }

    private List<RegionInfo> getAllRegions(TableName tableName, Admin admin) throws IOException {
        List<RegionInfo> allRegions = admin.getRegions(tableName);
        log.debug("found {} regions for table {}", allRegions.size(), tableName);
        return allRegions;
    }

    private Map<String, List<String>> refreshRegionToHostNameMapping(Admin admin, TableName tableName,
                                                                     List<String> allRegions,
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
        log.info("there are {} keys present in region-To-Favoured-Node mapping,ideally it should be total number of regions", regionFNHostnameMapping.size());
        return regionFNHostnameMapping;
    }

    private Scan getScan(Optional<PrefixFilter> filterOptional) {
        Scan scan = new Scan();
        scan.addColumn(NaiveRegionSelectionPolicy.INFO_CF, NaiveRegionSelectionPolicy.FN_CQ);
        filterOptional.ifPresent(scan::setFilter);
        return scan;
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

    private List<String> getEligibleRegions(Map<String, List<String>> regionFNHostnameMapping,
                                            Set<String> compactingRegions, List<String> allRegions) throws IOException {
        List<String> encodedRegions = new ArrayList<>();
        Map<String, MutableInt> serversForThisBatch = new WeakHashMap<>();
        for (String encodedRegion : compactingRegions) {
            if (regionFNHostnameMapping.containsKey(encodedRegion)) {
                regionFNHostnameMapping.get(encodedRegion).forEach(server -> {
                    serversForThisBatch.putIfAbsent(server, new MutableInt(0));
                    serversForThisBatch.get(server).increment();
                    log.debug("found {} compactions running for server {}", serversForThisBatch.get(server), server);
                });
            }
        }
        log.debug("starting to analyse {} regions - this is total number of regions present for this target", allRegions.size());
        for (String encodedRegion : allRegions) {
            if (!compactingRegions.contains(encodedRegion) && encodedRegions.size() < (
                    MAX_PARALLEL_COMPACTION_PER_TARGET - compactingRegions.size())) {
                if (!regionFNHostnameMapping.containsKey(encodedRegion)) {
                    log.warn("No favored nodes for region: " + encodedRegion);
                }
                boolean shouldAdd = true;
                for (String fn : regionFNHostnameMapping.get(encodedRegion)) {
                    if (serversForThisBatch.containsKey(fn) && serversForThisBatch.get(fn).intValue() >= MAX_PARALLEL_COMPACTION_PER_SERVER) {
                        log.debug("max parallel compaction count reached for this RS {}, hence not adding {}", fn, encodedRegion);
                        shouldAdd = false;
                        break;
                    }
                }
                if (shouldAdd && regionFNHostnameMapping.containsKey(encodedRegion)) {
                    regionFNHostnameMapping.get(encodedRegion).forEach(server -> {
                        serversForThisBatch.putIfAbsent(server, new MutableInt(0));
                        serversForThisBatch.get(server).increment();
                        log.debug("setting {} scheduled compactions for server {}", serversForThisBatch.get(server), server);
                    });
                    encodedRegions.add(encodedRegion);
                }
            }
        }
        log.info("returning {} regions as eligible regions for compaction in this run", encodedRegions.size());
        return  encodedRegions;
    }

}
