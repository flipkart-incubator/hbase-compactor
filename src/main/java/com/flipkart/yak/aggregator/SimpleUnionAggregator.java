package com.flipkart.yak.aggregator;


import com.flipkart.yak.commons.ConnectionInventory;
import com.flipkart.yak.commons.RegionEligibilityStatus;
import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.core.CompactionRuntimeException;
import com.flipkart.yak.core.PolicyRunner;
import com.flipkart.yak.interfaces.PolicyAggregator;
import com.flipkart.yak.interfaces.RegionSelectionPolicy;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Pair;

import java.util.List;
import java.util.Set;


@Slf4j
public class SimpleUnionAggregator implements PolicyAggregator {


    @Override
    public Report applyAndCollect(Set<RegionSelectionPolicy> allPolicies, PolicyRunner runner, CompactionContext compactionContext) throws CompactionRuntimeException {
        Connection connection = ConnectionInventory.getInstance().get(compactionContext.getClusterID());
        Report finalReport = new Report(this.getClass().getName());
        for (RegionSelectionPolicy regionSelectionPolicy : allPolicies) {
            Report report = regionSelectionPolicy.getReport(compactionContext, connection);
            report.forEach((k,v) -> {
                if(v.getSecond() == RegionEligibilityStatus.GREEN) {
                    finalReport.put(k,v);
                }
            });
        }
        log.info("total {} regions are being selected for major compaction in this run", finalReport.size());
        return finalReport;
    }

    @Override
    public void setFromConfig(List<Pair<String, String>> configs) {

    }
}
