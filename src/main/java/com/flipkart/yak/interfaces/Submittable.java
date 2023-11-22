package com.flipkart.yak.interfaces;


import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionSchedule;
import org.apache.commons.configuration.ConfigurationException;


public interface Submittable extends Runnable {
    void init(CompactionContext compactionContext) throws ConfigurationException;
    void setThreadName(CompactionContext compactionContext, CompactionSchedule compactionSchedule);
}
