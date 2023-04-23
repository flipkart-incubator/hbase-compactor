package com.flipkart.yak.policies;

import com.flipkart.yak.commons.ConnectionInventory;
import com.flipkart.yak.config.CompactionContext;
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

/**
 * Simple implementation of {@link com.flipkart.yak.interfaces.RegionSelectionPolicy } that controls number of maximum
 * compaction can be run on a Single RegionServer as well as on a Table. The number for max compaction per node defaults
 * to 1 and can be controlled by config key "compactor.policy.max.parallel.compaction.per.server".
 */
@Slf4j
public class NaiveRegionSelectionPolicy extends HBASEBasePolicy {

    private final static String HBASE_NS = "hbase";
    private final static String META_TABLE = "meta";
    private final static byte[] INFO_CF = Bytes.toBytes("info");
    private final static byte[] FN_CQ = Bytes.toBytes("fn");

    public static String KEY_MAX_PARALLEL_COMPACTION = "compactor.policy.max.parallel.compaction.per.server";
    public static String KEY_MAX_PARALLEL_COMPACTION_FOR_TABLE = "compactor.policy.max.parallel.compaction.per.table";
    private int MAX_PARALLEL_COMPACTION_PER_SERVER = 1;
    private int MAX_PARALLEL_COMPACTION_PER_TARGET = 5;

    @Override
    public void setFromConfig(List<Pair<String, String>> configs) {
        if(configs!= null) {
            configs.forEach(pair -> {
                if (pair.getFirst().equals(KEY_MAX_PARALLEL_COMPACTION)) {
                    MAX_PARALLEL_COMPACTION_PER_SERVER = Integer.parseInt(pair.getSecond());
                }

                if (pair.getFirst().equals(KEY_MAX_PARALLEL_COMPACTION_FOR_TABLE)) {
                    MAX_PARALLEL_COMPACTION_PER_TARGET = Integer.parseInt(pair.getSecond());
                }
            });
        }
        else {
            log.warn("config passed to this policy is null, please check config file");
        }
        log.info("Max Parallel Compaction for a server allowed {}", MAX_PARALLEL_COMPACTION_PER_SERVER);
        log.info("Max Parallel Compaction for a target allowed {}", MAX_PARALLEL_COMPACTION_PER_TARGET);
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

    //TODO: FavoredNode information can be taken into account while doing calculation for parallel job for a server
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

    private Scan getScan(Optional<PrefixFilter> filterOptional) {
        Scan scan = new Scan();
        scan.addColumn(NaiveRegionSelectionPolicy.INFO_CF, NaiveRegionSelectionPolicy.FN_CQ);
        filterOptional.ifPresent(scan::setFilter);
        return scan;
    }

    List<String> getEligibleRegions(Map<String, List<String>> regionFNHostnameMapping,
                                            Set<String> compactingRegions, List<RegionInfo> allRegions,
                                    Connection connection) throws IOException {
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
        for (RegionInfo region : allRegions) {
            if (!compactingRegions.contains(region.getEncodedName()) && encodedRegions.size() < (
                    MAX_PARALLEL_COMPACTION_PER_TARGET - compactingRegions.size())) {
                if (!regionFNHostnameMapping.containsKey(region.getEncodedName())) {
                    log.warn("No favored nodes for region: " + region.getEncodedName());
                }
                boolean shouldAdd = true;
                for (String fn : regionFNHostnameMapping.get(region.getEncodedName())) {
                    if (serversForThisBatch.containsKey(fn) && serversForThisBatch.get(fn).intValue() >= MAX_PARALLEL_COMPACTION_PER_SERVER) {
                        log.debug("max parallel compaction count reached for this RS {}, hence not adding {}", fn, region.getEncodedName());
                        shouldAdd = false;
                        break;
                    }
                }
                if (shouldAdd && regionFNHostnameMapping.containsKey(region.getEncodedName())) {
                    regionFNHostnameMapping.get(region.getEncodedName()).forEach(server -> {
                        serversForThisBatch.putIfAbsent(server, new MutableInt(0));
                        serversForThisBatch.get(server).increment();
                        log.debug("setting {} scheduled compactions for server {}", serversForThisBatch.get(server), server);
                    });
                    encodedRegions.add(region.getEncodedName());
                }
            }
        }
        log.info("returning {} regions as eligible regions for compaction in this run", encodedRegions.size());
        return  encodedRegions;
    }

    @Override
    public Connection init(CompactionContext compactionContext) {
        return ConnectionInventory.getInstance().get(compactionContext.getClusterID());
    }

    @Override
    public void release(Connection connection) {

    }
}
