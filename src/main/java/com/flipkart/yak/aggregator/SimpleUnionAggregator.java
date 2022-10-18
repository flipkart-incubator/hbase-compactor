package com.flipkart.yak.aggregator;

import com.flipkart.yak.commons.RegionEligibilityStatus;
import com.flipkart.yak.commons.Report;
import com.flipkart.yak.interfaces.PolicyAggregator;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Pair;

import java.util.List;
import java.util.Map;

@Slf4j
public class SimpleUnionAggregator implements PolicyAggregator {

    @Override
    public void setFromConfig(List<Pair<String, String>> configs) {

    }

    @Override
    public Report aggregateReport(List<Report> reports) {
        Report finalReport = new Report();
        log.info("aggregating {} reports ", reports.size());
        for (Report report : reports) {
            for (Map.Entry<String, Pair<RegionInfo, RegionEligibilityStatus>> region : report.entrySet()) {
                if (region.getValue().getSecond() == RegionEligibilityStatus.GREEN) {
                    finalReport.put(region.getKey(), region.getValue());
                    log.debug("adding region {} in final report from table {}", region.getKey(), region.getValue().getFirst().getTable());
                }
            }
        }
        log.info("total number of regions eligible for this run {}", finalReport.size());
        return  finalReport;
    }
}
