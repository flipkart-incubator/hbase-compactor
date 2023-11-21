package com.flipkart.yak.core;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.commons.ProfileInventoryFactory;
import com.flipkart.yak.commons.ScheduleUtils;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionSchedule;
import com.flipkart.yak.interfaces.CompactionExecutable;
import com.flipkart.yak.interfaces.ProfileInventory;
import com.flipkart.yak.interfaces.Submittable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.MDC;

import java.time.Instant;

/**
 * A compaction task responsible for executing compaction based on results returned by {@link com.flipkart.yak.interfaces.RegionSelectionPolicy}
 */
@Slf4j
@Getter
public class ThreadedCompactionJob implements Submittable {

    CompactionContext compactionContext;
    CompactionSchedule compactionSchedule;
    CompactionManager compactionManager;
    CompactionExecutable compactionExecutable;


    @Override
    public void init(CompactionContext context) throws ConfigurationException {
       log.info("loading compaction job: {} with context {}", this.getClass().getName(), context);
       compactionSchedule = context.getCompactionSchedule();
       compactionContext = context;
       compactionExecutable = new RegionByRegionThreadedCompactionJob();
       compactionExecutable.initResources(compactionContext);
       ProfileInventory profileInventory = ProfileInventoryFactory.getProfileInventory();
       compactionManager = new CompactionManager(compactionSchedule, compactionContext, compactionExecutable, profileInventory);
    }

    @Override
    public void run() {
        if(compactionSchedule.isPrompt()) {
            Thread.currentThread().setName(compactionContext.getTableName()+"-"+compactionContext.getNameSpace()+"-"+"Prompt");
        } else {
            Thread.currentThread().setName(compactionContext.getTableName()+"-"+compactionContext.getNameSpace());
        }
        MDC.put("JOB", Thread.currentThread().getName());
        log.info("starting compact-cron for : {}", this.getCompactionContext());
        while(true) {

             /*
            If prompt job , and its life cycle has already ended , exit
             */
            if(compactionSchedule.isPrompt() && ScheduleUtils.hasExpired(compactionSchedule, Instant.now())) {
                Thread.currentThread().interrupt();
                this.compactionExecutable.releaseResources();
                break;
            }
            MonitorService.resetMeterValue(compactionExecutable.getClass(), compactionContext, "success");
            MonitorService.resetMeterValue(compactionExecutable.getClass(), compactionContext, "failure");
            this.compactionManager.checkAndStart();

            /*
            If prompt job , exit from schedule after completing compaction
             */
            if(compactionSchedule.isPrompt()) {
                Thread.currentThread().interrupt();
                this.compactionExecutable.releaseResources();
                break;
            }

            /*
            If not prompt , Halt until next schedule
             */
            long sleepFor = ScheduleUtils.getSleepTime(compactionSchedule);
            log.info("sleeping for {}", sleepFor);
            try {
                Thread.sleep(sleepFor);
            } catch (InterruptedException e) {
                log.error("sleep-wait interrupted, exiting ..!");
                Thread.currentThread().interrupt();
                this.compactionExecutable.releaseResources();
                break;
            }
        }
    }
}
