package com.flipkart.yak.core;


import com.flipkart.yak.commons.ConnectionInventory;
import com.flipkart.yak.commons.RegionEligibilityStatus;
import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.interfaces.CompactionExecutable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class RegionByRegionThreadedCompactionJob implements CompactionExecutable {

    private Connection connection;
    private Admin admin;
    private CompactionContext compactionContext;

    @Override
    public void initResources(CompactionContext context) throws ConfigurationException {
        try {
            this.connection = ConnectionInventory.getInstance().get(context.getClusterID());
            this.admin = this.connection.getAdmin();
            this.compactionContext = context;
        } catch (IOException e) {
            throw new ConfigurationException(e.getMessage());
        }
    }

    @Override
    public void doCompact(Report report) throws CompactionRuntimeException {
        log.debug("received {} regions for compaction", report.size());
        for (Map.Entry<String, Pair<RegionInfo, RegionEligibilityStatus>> entry : report.entrySet()) {
            try {
                log.info("calling major compaction for {}", entry.getKey());
                this.admin.majorCompactRegion(entry.getValue().getFirst().getEncodedNameAsBytes());
                MonitorService.reportValue(this.getClass(), this.compactionContext, "success",1);
            } catch (IOException e) {
                log.error("Could not trigger compaction for {}", entry.getKey());
                MonitorService.reportValue(this.getClass(), this.compactionContext, "failure",1);
                throw new CompactionRuntimeException(e);
            }
        }
    }

    @Override
    public void releaseResources() {
        try {
            this.connection.close();
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }
}
