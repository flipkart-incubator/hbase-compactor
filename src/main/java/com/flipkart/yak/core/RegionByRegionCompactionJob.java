package com.flipkart.yak.core;


import com.flipkart.yak.commons.RegionEligibilityStatus;
import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class RegionByRegionCompactionJob extends CompactionJob{

    private Admin admin;

    @Override
    public void init(CompactionContext context) throws ConfigurationException {
        super.init(context);
        try {
            this.admin = this.getConnection().getAdmin();
        } catch (IOException e) {
            throw new ConfigurationException(e.getMessage());
        }
    }

    @Override
    void doCompact(Report report) throws CompactionRuntimeException {
        for (Map.Entry<String, Pair<RegionInfo, RegionEligibilityStatus>> entry : report.entrySet()) {
            try {
                this.admin.majorCompactRegion(entry.getValue().getFirst().getEncodedNameAsBytes());
            } catch (IOException e) {
                log.error("Could not trigger compaction for {}", entry.getKey());
                throw new CompactionRuntimeException(e);
            }
        }
    }

    @Override
    void releaseResources() {
        try {
            this.getConnection().close();
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }
}
