package com.flipkart.yak.core;

import com.flipkart.yak.commons.CompactionProfile;
import com.flipkart.yak.commons.ProfileInventoryFactory;
import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionSchedule;
import com.flipkart.yak.interfaces.CompactionExecutable;
import com.flipkart.yak.interfaces.PolicyAggregator;
import com.flipkart.yak.interfaces.RegionSelectionPolicy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.time.DateUtils;

import java.util.Set;

@Slf4j
@Getter
public class CompactionManager {
    private Set<RegionSelectionPolicy> policies;
    private PolicyAggregator aggregator;
    private final CompactionSchedule compactionSchedule;
    private final CompactionContext compactionContext;
    private long DEFAULT_DELAY_BETWEEN_EACH_RUN = DateUtils.MILLIS_PER_MINUTE;
    private final CompactionExecutable compactionExecutable;

    public CompactionManager(CompactionSchedule compactionSchedule, CompactionContext compactionContext, CompactionExecutable compactionExecutable) throws ConfigurationException {
        this.compactionSchedule = compactionSchedule;
        this.compactionContext = compactionContext;
        CompactionProfile profileForThis = ProfileInventoryFactory.getProfileInventory().get(compactionContext.getCompactionProfileID());
        aggregator = profileForThis.getAggregator();
        policies = profileForThis.getPolicies();
        this.compactionExecutable = compactionExecutable;
    }

    public final void checkAndStart() {
        while (!ScheduleUtils.hasTimedOut(compactionSchedule) && ScheduleUtils.canStart(compactionSchedule)) {
            try {
                Report report = this.aggregateReport();
                this.compactionExecutable.doCompact(report);
                log.debug("sleeping for {}", DEFAULT_DELAY_BETWEEN_EACH_RUN);
                Thread.sleep(DEFAULT_DELAY_BETWEEN_EACH_RUN);
            } catch (CompactionRuntimeException | InterruptedException e) {
                log.error("exception while trying to trigger compaction :{} \n . will start again.", e.getMessage());
            }
        }
    }

    private Report aggregateReport() throws CompactionRuntimeException {
        return this.aggregator.applyAndCollect(this.getPolicies(), this.compactionContext);
    }
}
