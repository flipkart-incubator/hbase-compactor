package com.flipkart.yak.config;

import com.flipkart.yak.core.CompactionRuntimeException;
import com.flipkart.yak.core.JobSubmitter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;

/**
 * Listener interface to be extended by specific implementation. Implementing class should define onChange() and listen().
 * listen() method initializes the class and bootstraps the watcher.
 * onChange() method contains actions to be taken when there is a change in Data.
 */
@Slf4j
public abstract class ConfigListener {

    JobSubmitter jobSubmitter;
    protected abstract void onChange() throws ConfigurationException;

    public abstract void listen() throws CompactionRuntimeException, ConfigurationException;
    public final void register(JobSubmitter jobSubmitter) {
        this.jobSubmitter = jobSubmitter;
    }

    /**
     * Sends signal to {@link JobSubmitter}, i.e. task container to reload all tasks.
     * @param compactionTriggerConfig new {@link CompactionTriggerConfig} which is loaded by Store-Data change event.
     */
    public final void restart(CompactionTriggerConfig compactionTriggerConfig){
        if(jobSubmitter != null) {
            try {
                jobSubmitter.reload(compactionTriggerConfig);
            } catch (CompactionRuntimeException e) {
                log.error("{}", e.getMessage());
                throw new RuntimeException(e);
            }
        }
        else {
         throw new RuntimeException("JobSubmitter Not initialised");
        }
    }
}
