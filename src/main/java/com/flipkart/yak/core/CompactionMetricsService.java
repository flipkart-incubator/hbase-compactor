package com.flipkart.yak.core;

import com.flipkart.yak.config.CompactionContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Service responsible for collecting and exposing compaction-related metrics
 * for monitoring and alerting purposes.
 */
@Slf4j
public class CompactionMetricsService {

    private static final long THREE_DAYS_MILLIS = TimeUnit.DAYS.toMillis(3);

    private static final long NEVER_COMPACTED_GRACE_PERIOD_MILLIS = TimeUnit.HOURS.toMillis(24);

    private static final ConcurrentHashMap<String, Map<String, TableCompactionMetrics>> tableMetricsCache = new ConcurrentHashMap<>();
    
    /**
     * Collect and update table-level compaction metrics for the given context
     */
    public static void updateMetrics(CompactionContext context, Connection connection, List<RegionInfo> allRegions) {
        log.debug("Updating table-level compaction metrics for context: {}", context);
        try (Admin admin = connection.getAdmin()) {
            long currentTime = EnvironmentEdgeManager.currentTime();
            

            Map<String, TableCompactionMetrics> tableMetrics = calculateTableMetrics(admin, allRegions, currentTime, context);
            String cacheKey = createCacheKey(context);
            tableMetricsCache.put(cacheKey, tableMetrics);

            registerTableGaugeMetrics(context, cacheKey, tableMetrics);

            updateTableCounterMetrics(context, tableMetrics);

            logTableLevelProblematicRegions(context, tableMetrics);
            

            int totalRegions = tableMetrics.values().stream().mapToInt(tm -> tm.totalRegions).sum();
            int totalProblematicRegions = tableMetrics.values().stream().mapToInt(tm -> tm.regionsNotCompactedIn3Days).sum();
            double overallPercentage = totalRegions > 0 ? (double) totalProblematicRegions / totalRegions * 100.0 : 0.0;
            
            log.info("Updated table-level compaction metrics for context {}: {} tables, {} total regions, {} problematic regions ({}%)", 
                    context, tableMetrics.size(), totalRegions, totalProblematicRegions, 
                    String.format("%.2f", overallPercentage));
                    
        } catch (IOException e) {
            log.error("Failed to update compaction metrics for context {}: {}", context, e.getMessage(), e);
        } catch (Exception e) {
            log.error("Unexpected error while updating compaction metrics for context {}: {}", context, e.getMessage(), e);
        }
    }
    
    /**
     * Calculate table-level compaction metrics
     */
    private static Map<String, TableCompactionMetrics> calculateTableMetrics(Admin admin, List<RegionInfo> allRegions, long currentTime, CompactionContext context) throws IOException {
        Map<String, TableCompactionMetrics> tableMetrics = new HashMap<>();

        Map<String, List<RegionInfo>> regionsByTable = allRegions.stream()
            .collect(Collectors.groupingBy(region -> region.getTable().getNameAsString()));
        
        for (Map.Entry<String, List<RegionInfo>> entry : regionsByTable.entrySet()) {
            String tableName = entry.getKey();
            List<RegionInfo> tableRegions = entry.getValue();
            
            int totalRegions = tableRegions.size();
            int regionsNotCompactedIn3Days = 0;
            List<String> regionsNotCompactedIn3DaysList = new ArrayList<>();
            int regionsWithCompactionHistory = 0;
            long oldestCompactionTime = currentTime;
            long newestCompactionTime = 0;

            List<RegionInfo> neverCompactedRegions = new ArrayList<>();
            
            for (RegionInfo region : tableRegions) {
                try {
                    long lastCompactionTime = admin.getLastMajorCompactionTimestampForRegion(region.getRegionName());
                    
                    if (lastCompactionTime > 0) {
                        regionsWithCompactionHistory++;
                        long timeSinceLastCompaction = currentTime - lastCompactionTime;

                        if (timeSinceLastCompaction > THREE_DAYS_MILLIS) {
                            regionsNotCompactedIn3Days++;
                            regionsNotCompactedIn3DaysList.add(region.getEncodedName());
                        }
                        
                        if (lastCompactionTime < oldestCompactionTime) {
                            oldestCompactionTime = lastCompactionTime;
                        }
                        if (lastCompactionTime > newestCompactionTime) {
                            newestCompactionTime = lastCompactionTime;
                        }
                    } else {
                        neverCompactedRegions.add(region);
                    }
                } catch (Exception e) {
                    log.warn("Failed to get compaction timestamp for region {} in table {}: {}", 
                            region.getEncodedName(), tableName, e.getMessage());
                    regionsNotCompactedIn3Days++;
                    regionsNotCompactedIn3DaysList.add(region.getEncodedName());
                }
            }
            for (RegionInfo region : neverCompactedRegions) {
                if (shouldFlagNeverCompactedRegion(currentTime, newestCompactionTime, regionsWithCompactionHistory)) {
                    regionsNotCompactedIn3Days++;
                    regionsNotCompactedIn3DaysList.add(region.getEncodedName());
                }
            }
            double percentageNotCompactedIn3Days = totalRegions > 0 ? 
                (double) regionsNotCompactedIn3Days / totalRegions * 100.0 : 0.0;
            
            TableCompactionMetrics tableMetric = new TableCompactionMetrics(
                tableName,
                totalRegions,
                regionsNotCompactedIn3Days,
                percentageNotCompactedIn3Days,
                regionsNotCompactedIn3DaysList
            );
            
            tableMetrics.put(tableName, tableMetric);
        }
        return tableMetrics;
    }
    
    /**
     * Log table-level problematic regions for alerting purposes
     */
    private static void logTableLevelProblematicRegions(CompactionContext context, Map<String, TableCompactionMetrics> tableMetrics) {
        for (TableCompactionMetrics tableMetric : tableMetrics.values()) {
            if (tableMetric.regionsNotCompactedIn3Days == 0) {
                log.info("Table '{}' is healthy: all {} regions compacted within 3 days",
                        tableMetric.tableName, tableMetric.totalRegions);
                continue;
            }


            log.warn(" TABLE_COMPACTION_ALERT: Table '{}' has {} out of {} regions ({}%) not compacted in last 3 days. " +
                    "Context: {}. Problematic regions: [{}]", 
                    tableMetric.tableName,
                    tableMetric.regionsNotCompactedIn3Days,
                    tableMetric.totalRegions,
                    String.format("%.1f", tableMetric.percentageNotCompactedIn3Days),
                    context,
                    String.join(", ", tableMetric.regionsNotCompactedIn3DaysList));
        }
    }
    
    /**
     * Register table-level gauge metrics
     */
    private static void registerTableGaugeMetrics(CompactionContext context, String cacheKey, Map<String, TableCompactionMetrics> tableMetrics) {
        for (String tableName : tableMetrics.keySet()) {
            String tableMetricPrefix = "table_" + sanitizeTableName(tableName) + "_";

            MonitorService.registerGauge(CompactionMetricsService.class, context, 
                tableMetricPrefix + "regionsNotCompactedIn3DaysPercentage", 
                () -> {
                    Map<String, TableCompactionMetrics> currentTableMetrics = tableMetricsCache.get(cacheKey);
                    if (currentTableMetrics != null && currentTableMetrics.containsKey(tableName)) {
                        return currentTableMetrics.get(tableName).percentageNotCompactedIn3Days;
                    }
                    return 0.0;
                });
        }
    }
    
    /**
     * Update table-level counter metrics
     */
    private static void updateTableCounterMetrics(CompactionContext context, Map<String, TableCompactionMetrics> tableMetrics) {
        for (TableCompactionMetrics tableMetric : tableMetrics.values()) {
            String tableMetricPrefix = "table_" + sanitizeTableName(tableMetric.tableName) + "_";

            MonitorService.setCounterValue(CompactionMetricsService.class, context, 
                tableMetricPrefix + "totalRegions", tableMetric.totalRegions);

            MonitorService.setCounterValue(CompactionMetricsService.class, context, 
                tableMetricPrefix + "regionsNotCompactedIn3Days", tableMetric.regionsNotCompactedIn3Days);
        }
    }
    
    /**
     * Sanitizes table name for use in JMX metric names
     * Only replaces characters that would break JMX ObjectName parsing
     * JMX reserved characters: : , = * ? and spaces
     */
    private static String sanitizeTableName(String tableName) {
        return tableName
            .replace(":", "_")      // namespace separator
            .replace(",", "_")      // JMX property separator  
            .replace("=", "_")      // JMX key-value separator
            .replace("*", "_")      // JMX wildcard
            .replace("?", "_")      // JMX wildcard
            .replace(" ", "_");     // spaces can cause parsing issues
    }

    /**
     * Get table-level compaction metrics for a specific context
     */
    public static Map<String, TableCompactionMetrics> getTableMetrics(CompactionContext context) {
        String cacheKey = createCacheKey(context);
        Map<String, TableCompactionMetrics> tableMetrics = tableMetricsCache.get(cacheKey);
        return tableMetrics != null ? new HashMap<>(tableMetrics) : new HashMap<>();
    }
    
    /**
     * Get compaction metrics for a specific table
     */
    public static TableCompactionMetrics getTableMetrics(CompactionContext context, String tableName) {
        Map<String, TableCompactionMetrics> allTableMetrics = getTableMetrics(context);
        return allTableMetrics.get(tableName);
    }
    
    /**
     * Get list of problematic regions for a specific table
     */
    public static List<String> getProblematicRegionsForTable(CompactionContext context, String tableName) {
        TableCompactionMetrics tableMetrics = getTableMetrics(context, tableName);
        return tableMetrics != null ? new ArrayList<>(tableMetrics.regionsNotCompactedIn3DaysList) : new ArrayList<>();
    }
    
    /**
     * Get list of all tables that have compaction issues
     */
    public static List<String> getTablesWithIssues(CompactionContext context) {
        return getTableMetrics(context).values().stream()
            .filter(tm -> !tm.isHealthy())
            .map(tm -> tm.tableName)
            .collect(Collectors.toList());
    }
    
    /**
     * Data class to hold table-specific compaction metrics
     */
    public static final class TableCompactionMetrics {
        public final String tableName;
        public final int totalRegions;
        public final int regionsNotCompactedIn3Days;
        public final double percentageNotCompactedIn3Days;
        public final List<String> regionsNotCompactedIn3DaysList;
        
        public TableCompactionMetrics(String tableName, int totalRegions, int regionsNotCompactedIn3Days,
                                    double percentageNotCompactedIn3Days, List<String> regionsNotCompactedIn3DaysList) {
            this.tableName = tableName;
            this.totalRegions = totalRegions;
            this.regionsNotCompactedIn3Days = regionsNotCompactedIn3Days;
            this.percentageNotCompactedIn3Days = percentageNotCompactedIn3Days;
            this.regionsNotCompactedIn3DaysList = regionsNotCompactedIn3DaysList != null ? regionsNotCompactedIn3DaysList : new ArrayList<>();
        }
        
        public boolean isHealthy() {
            return regionsNotCompactedIn3Days == 0;
        }
        
        public boolean hasCriticalIssues() {
            return percentageNotCompactedIn3Days > 50.0;
        }
        
        @Override
        public String toString() {
            return String.format("TableMetrics{table='%s', total=%d, problematic=%d (%.1f%%)}", 
                tableName, totalRegions, regionsNotCompactedIn3Days, percentageNotCompactedIn3Days);
        }
    }
    
    /**
     * Data class representing a problematic region
     */
    public static class ProblematicRegion {
        public final String encodedName;
        public final String tableName;
        public final long lastCompactionTimestamp;
        public final long timeSinceLastCompactionMs;
        
        public ProblematicRegion(String encodedName, String tableName, long lastCompactionTimestamp, long timeSinceLastCompactionMs) {
            this.encodedName = encodedName;
            this.tableName = tableName;
            this.lastCompactionTimestamp = lastCompactionTimestamp;
            this.timeSinceLastCompactionMs = timeSinceLastCompactionMs;
        }
        
        public boolean isNeverCompacted() {
            return timeSinceLastCompactionMs == -1;
        }
        
        public boolean isErrorGettingTimestamp() {
            return timeSinceLastCompactionMs == -2;
        }
        
        public long getDaysSinceLastCompaction() {
            return timeSinceLastCompactionMs > 0 ? TimeUnit.MILLISECONDS.toDays(timeSinceLastCompactionMs) : 0;
        }
        
        @Override
        public String toString() {
            if (isNeverCompacted()) {
                return String.format("Region{%s, table=%s, status=NEVER_COMPACTED}", encodedName, tableName);
            } else if (isErrorGettingTimestamp()) {
                return String.format("Region{%s, table=%s, status=ERROR_GETTING_TIMESTAMP}", encodedName, tableName);
            } else {
                return String.format("Region{%s, table=%s, daysSinceCompaction=%d}", 
                    encodedName, tableName, getDaysSinceLastCompaction());
            }
        }
    }
    
    /**
     * Determines if a never-compacted region should be flagged as problematic.
     * Uses table activity heuristics to avoid flagging newly created regions.
     * 
     * @param currentTime Current timestamp
     * @param newestCompactionTime Most recent compaction timestamp in the context
     * @param regionsWithCompactionHistory Number of regions that have been compacted at least once
     * @return true if the never-compacted region should be considered problematic
     */
    private static boolean shouldFlagNeverCompactedRegion(long currentTime, long newestCompactionTime, int regionsWithCompactionHistory) {
        if (regionsWithCompactionHistory == 0) {
            return false;
        }
        long timeSinceNewestCompaction = currentTime - newestCompactionTime;
        return timeSinceNewestCompaction >= NEVER_COMPACTED_GRACE_PERIOD_MILLIS;
    }
    
    /**
     * Create a cache key for the given context
     * Handles null/empty tableNames consistently (represents "all tables in namespace")
     */
    private static String createCacheKey(CompactionContext context) {
        String tableNames = context.getTableNames();
        if (tableNames == null || tableNames.trim().isEmpty()) {
            tableNames = "ALL_TABLES_IN_NAMESPACE";
        }
        
        return context.getClusterID() + "_" + context.getNameSpace() + "_" + 
               context.getRsGroup() + "_" + tableNames;
    }
    
}
