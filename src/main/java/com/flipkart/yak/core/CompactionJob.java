package com.flipkart.yak.core;

import com.flipkart.yak.commons.CompactionProfile;
import com.flipkart.yak.commons.ConnectionInventory;
import com.flipkart.yak.commons.ProfileInventoryFactory;
import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionSchedule;
import com.flipkart.yak.interfaces.PolicyAggregator;
import com.flipkart.yak.interfaces.RegionSelectionPolicy;
import com.flipkart.yak.interfaces.Submittable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.*;


@Slf4j
@Getter
public abstract class CompactionJob implements Submittable {

    private Set<RegionSelectionPolicy> policies;
    private PolicyAggregator aggregator;
    private CompactionSchedule compactionSchedule;
    private CompactionContext compactionContext;
    private long DEFAULT_DELAY_BETWEEN_EACH_RUN = DateUtils.MILLIS_PER_MINUTE;

    @Override
    public void init(CompactionContext context) throws ConfigurationException {
       log.info("loading compaction job: {} with context {}", this.getClass().getName(), context);
       compactionSchedule = context.getCompactionSchedule();
       CompactionProfile profileForThis = ProfileInventoryFactory.getProfileInventory().get(context.getCompactionProfileID());
       aggregator = profileForThis.getAggregator();
       policies = profileForThis.getPolicies();
       compactionContext = context;

    }

    @Override
    public void run() {
        Thread.currentThread().setName(compactionContext.getTableName()+"-"+compactionContext.getNameSpace());
        MDC.put("JOB", Thread.currentThread().getName());
        log.info("starting compact-cron for : {}", this.getCompactionContext());

        while(true) {
            while (!this.hasTimedOut(compactionSchedule) && this.canStart(compactionSchedule)) {
                try {
                    Report report = this.aggregateReport();
                    this.doCompact(report);
                    Thread.sleep(DEFAULT_DELAY_BETWEEN_EACH_RUN);
                } catch (CompactionRuntimeException | InterruptedException e) {
                    log.error("exception while trying to trigger compaction :{} \n . will start again.", e.getMessage());
                }
            }
            long sleepFor = this.getSleepTime(compactionSchedule);
            log.debug("sleeping for {}", sleepFor);
            try {
                Thread.sleep(sleepFor);
            } catch (InterruptedException e) {
                log.error("sleep-wait interrupted, exiting ..!");
                this.releaseResources();
                break;
            }
        }
    }

    private Report aggregateReport() throws CompactionRuntimeException {
        return this.aggregator.applyAndCollect(this.getPolicies(), this.compactionContext);
    }

    private long  getStartOfTheDay() {
        Calendar calendar = DateUtils.toCalendar(Date.from(Instant.now()));
        Calendar todaysDate = DateUtils.truncate(calendar, Calendar.DATE);
        return todaysDate.getTimeInMillis();
    }

    private long getSleepTime(CompactionSchedule compactionSchedule) {
        long baseTime = this.getStartOfTheDay();
        long startTime = baseTime + ((long)compactionSchedule.getStartHourOfTheDay() * DateUtils.MILLIS_PER_HOUR) + DateUtils.MILLIS_PER_DAY;
        long currTime = System.currentTimeMillis();
        if (currTime > startTime) {
            return -1;
        }
        log.debug("calculated thread sleep time {}", (startTime-currTime));
        return startTime - currTime;
    }

    private boolean hasTimedOut(CompactionSchedule compactionSchedule) {
        long baseTime = this.getStartOfTheDay();
        long currTIme = System.currentTimeMillis();
        long endTime = baseTime + ((long)compactionSchedule.getEndHourOfTheDay() * DateUtils.MILLIS_PER_HOUR);
        if (currTIme < endTime) {
            return false;
        }
        return true;
    }

    private boolean canStart(CompactionSchedule compactionSchedule) {
        long baseTime = this.getStartOfTheDay();
        long currTime = System.currentTimeMillis();
        long startTime = baseTime + ((long)compactionSchedule.getStartHourOfTheDay() * DateUtils.MILLIS_PER_HOUR);
        if (currTime >= startTime) {
            return true;
        }
        return false;
    }

    abstract void doCompact(Report report) throws CompactionRuntimeException;
    abstract void releaseResources();
}
