package com.flipkart.yak.core;

import com.flipkart.yak.commons.CompactionProfile;
import com.flipkart.yak.commons.ProfileInventoryFactory;
import com.flipkart.yak.commons.Report;
import com.flipkart.yak.commons.ScheduleUtils;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionSchedule;
import com.flipkart.yak.interfaces.CompactionExecutable;
import com.flipkart.yak.interfaces.PolicyAggregator;
import com.flipkart.yak.interfaces.ProfileInventory;
import com.flipkart.yak.interfaces.RegionSelectionPolicy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.util.Time;

import java.time.Instant;
import java.util.Set;

import static com.flipkart.yak.commons.ScheduleUtils.hasLifeCycleEnded;

@Slf4j
@Getter
public class CompactionManager {
    private Set<RegionSelectionPolicy> policies;
    private PolicyAggregator aggregator;
    private final CompactionSchedule compactionSchedule;
    private final CompactionContext compactionContext;
    private long DEFAULT_DELAY_BETWEEN_EACH_RUN = DateUtils.MILLIS_PER_MINUTE;
    private final CompactionExecutable compactionExecutable;
    private final PolicyRunner policyRunner = new PolicyRunner();

    public CompactionManager(CompactionSchedule compactionSchedule, CompactionContext compactionContext, CompactionExecutable compactionExecutable, ProfileInventory profileInventory) throws ConfigurationException {
        this.compactionSchedule = compactionSchedule;
        this.compactionContext = compactionContext;
        CompactionProfile profileForThis = profileInventory.get(compactionContext.getCompactionProfileID());
        aggregator = profileForThis.getAggregator();
        policies = profileForThis.getPolicies();
        this.compactionExecutable = compactionExecutable;
    }

    public final void checkAndStart() {
        while (!ScheduleUtils.hasTimedOut(compactionSchedule) && ScheduleUtils.canStart(compactionSchedule)) {
            if(compactionSchedule.isPrompt() && hasLifeCycleEnded(compactionSchedule, Instant.now())) {
                break;
            }
            try {
                Report report = this.aggregateReport();
                this.compactionExecutable.doCompact(report);
            } catch (CompactionRuntimeException e) {
                log.error("exception while trying to trigger compaction :{} \n . will try again in {} millis", e.getMessage(), DEFAULT_DELAY_BETWEEN_EACH_RUN);
            }
            log.debug("sleeping for {} millis", DEFAULT_DELAY_BETWEEN_EACH_RUN);
            try {
                Thread.sleep(DEFAULT_DELAY_BETWEEN_EACH_RUN);
            } catch (InterruptedException e) {
                log.error("wait interrupted {}",e.getMessage());
            }
        }
    }

    private Report aggregateReport() throws CompactionRuntimeException {
        return this.aggregator.applyAndCollect(this.getPolicies(), this.policyRunner, this.compactionContext);
    }
}
