package com.flipkart.yak.core;

import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionSchedule;
import com.flipkart.yak.interfaces.RegionSelectionPolicy;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Optional;
@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class PolicyRunnerTest {

    @Mock
    RegionSelectionPolicy regionSelectionPolicy;
    PolicyRunner policyRunner = new PolicyRunner();
    Report report;
    CompactionContext compactionContext;

    @Before
    public void setup() {
        CompactionSchedule compactionSchedule = new CompactionSchedule(18,20);
        compactionContext = new CompactionContext("xyz", compactionSchedule, "test");
        report = new Report("someRandomClass");
    }
    @Test
    public void simpleEmptyReportTest() {
        try {
            Mockito.when(regionSelectionPolicy.getReport(Mockito.any(), Mockito.any())).thenReturn(report);
        } catch (CompactionRuntimeException e) {
            log.error("Error in test: ", e.getMessage());
        }
        Report returnedReport = null;
        try {
            returnedReport = policyRunner.runPolicy(regionSelectionPolicy, compactionContext, Optional.empty());
        } catch (CompactionRuntimeException e) {
            log.error("Error in test: ", e.getMessage());
        }
        assert report.size() == returnedReport.size();
    }

    @Test
    public void simpleNonEmptyReportTest() {
        report.put("random", new Pair<>());
        try {
            Mockito.when(regionSelectionPolicy.getReport(Mockito.any(), Mockito.any())).thenReturn(report);
        } catch (CompactionRuntimeException e) {
            log.error("Error in test: ", e.getMessage());
        }
        Report returnedReport = null;
        try {
            returnedReport = policyRunner.runPolicy(regionSelectionPolicy, compactionContext, Optional.empty());
        } catch (CompactionRuntimeException e) {
            log.error("Error in test: ", e.getMessage());
        }
        assert report.size() == returnedReport.size();
    }
}
