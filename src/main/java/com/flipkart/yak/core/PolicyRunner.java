package com.flipkart.yak.core;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.interfaces.RegionSelectionPolicy;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
@Slf4j
public class PolicyRunner {

    private static PolicyRunner policyRunner = null;
    private Map<RegionSelectionPolicy, Object> resourceCache = new HashMap<>();

    public PolicyRunner() {

    }

    private Object getOrCreateResource(RegionSelectionPolicy regionSelectionPolicy, CompactionContext compactionContext) {
        resourceCache.putIfAbsent(regionSelectionPolicy, regionSelectionPolicy.init(compactionContext));
        return resourceCache.get(regionSelectionPolicy);
    }


    public Report runPolicy(RegionSelectionPolicy regionSelectionPolicy, CompactionContext compactionContext,
                            Optional<Report> baseReport) throws CompactionRuntimeException {
        Report report = null;
        try {
            Object resource = this.getOrCreateResource(regionSelectionPolicy, compactionContext);

            if (!baseReport.isPresent()) {
                log.debug("{}:{} No base report specified, hence proceeding with no filter", regionSelectionPolicy.getClass().getName(), compactionContext.getTableName());
                report = regionSelectionPolicy.getReport(compactionContext, resource);
            } else {
                report = regionSelectionPolicy.getReport(compactionContext,  resource, baseReport.get());
            }
            MonitorService.reportValue(regionSelectionPolicy.getClass(), compactionContext, "NumRegionInReport",report.size());
        } catch (CompactionRuntimeException e) {
            throw e;
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new CompactionRuntimeException(e);
        }
        if(report!= null) {
            log.info("{}:{} Report {}", compactionContext.getNameSpace(),compactionContext.getTableName(), report.size());
            if(log.isDebugEnabled()) {
                report.forEach((K,V) -> {
                    log.debug("{} {}", K,V.getSecond());
                });
            }
        } else {
            log.error("No report generated in this run, something is wrong. {}", compactionContext);
            throw new CompactionRuntimeException("Could not generate report!");
        }
        return report;
    }
}
