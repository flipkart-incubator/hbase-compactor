package com.flipkart.yak.commons;

import com.flipkart.yak.config.CompactionContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;

import java.io.IOException;
import java.util.*;
@Slf4j
public class HBaseUtils {

    public static List<RegionInfo> getRegionsAll(CompactionContext context, Admin admin) throws IOException {
        TableName tableName= TableName.valueOf(context.getNameSpace()+":"+context.getTableName());
        return admin.getRegions(tableName);
    }


    public static void refreshRegionToNodeMapping(Admin admin, List<String> listOfEncodedRegions,
                                                  Map<String, List<String>> regionFNHostnameMapping) throws IOException  {
        for (ServerName sn : admin.getRegionServers()) {
            List<RegionInfo> regions = admin.getRegions(sn);
            regions.forEach(region -> {
                if (listOfEncodedRegions.contains(region.getEncodedName())) {
                    regionFNHostnameMapping.putIfAbsent(region.getEncodedName(), new ArrayList<>());
                    regionFNHostnameMapping.get(region.getEncodedName()).add(sn.getHostname());
                }
            });
        }
    }

    public static Map<String, Set<RegionInfo>> getHostToRegionMapping(CompactionContext compactionContext, Admin admin) throws IOException  {
        Map<String, Set<RegionInfo>> hostToRegionMapping = new HashMap<>();
        List<RegionInfo> regionInfosForThisContext = getRegionsAll(compactionContext, admin);
        Set<RegionInfo> setOfRegionsForThisContext = new HashSet<>(regionInfosForThisContext);

            for (ServerName sn : admin.getRegionServers()) {
                try {
                    List<RegionInfo> regionInfosForThisServer = admin.getRegions(sn);
                    regionInfosForThisServer.forEach(region -> {
                        if (setOfRegionsForThisContext.contains(region)) {
                            hostToRegionMapping.putIfAbsent(sn.getHostname(), new HashSet<>());
                            hostToRegionMapping.get(sn.getHostname()).add(region);
                        }
                    });
                } catch (Exception e) {
                    log.error("could not get info for {}: Error: {}", sn.getHostname(), e.getMessage());
                }
            }

        return hostToRegionMapping;
    }
}
