package com.flipkart.yak.config;

import com.flipkart.yak.config.loader.AbstractConfigLoader;
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
    AbstractConfigLoader configLoader;

    public ConfigListener(AbstractConfigLoader configLoader) {
        this.configLoader = configLoader;
    }

    /**
     * Implementing class should call this method whenever there is a change detected.
     * @throws ConfigurationException if Unable to load new config
     */
    public void onChange() throws ConfigurationException{
        if (configLoader != null) {
            CompactionTriggerConfig config = configLoader.clearGetConfig();
            log.info("reloaded config.. ");
            log.info("loaded {} contexts and {} profiles.", config.getCompactionContexts().size(), config.getCompactionProfileConfigs().size());
            if (log.isDebugEnabled()) {
                for(CompactionProfileConfig compactionProfileConfig: config.getCompactionProfileConfigs()) {
                    log.debug("Loaded {}: {}: {}", compactionProfileConfig.getID(), compactionProfileConfig.getAggregator(), compactionProfileConfig.getPolicies().size());
                }
                log.debug("{}", config.getCompactionContexts());
            }
            this.restart(config);
        }
    }

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
