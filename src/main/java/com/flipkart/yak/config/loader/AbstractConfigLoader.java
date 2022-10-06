package com.flipkart.yak.config.loader;

import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfile;
import com.flipkart.yak.config.CompactionTriggerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public abstract class AbstractConfigLoader {
    CompactionTriggerConfig config = null;
    List<String> resourceNames = new ArrayList<>();


    abstract protected CompactionTriggerConfig loadConfig(String resourceName) throws ConfigurationException;

    /**
     * initializes the config, ignores if issue in loading config
     */
    private void init() {
        if (config == null) {
            List<CompactionTriggerConfig> compactionTriggerConfigs = new ArrayList<>();
            this.resourceNames.forEach(resource -> {
                try {
                    compactionTriggerConfigs.add(this.loadConfig(resource));
                } catch (ConfigurationException e) {
                    log.warn("Found issue with {} while loading.. ignoring this resource: {}", resource, e.getMessage());
                }
            });
            this.config = this.mergeConfig(compactionTriggerConfigs);
        }
    }

    private CompactionTriggerConfig mergeConfig(List<CompactionTriggerConfig> compactionTriggerConfigs) {
        CompactionTriggerConfig.Builder builder = new CompactionTriggerConfig.Builder();
        Set<CompactionProfile> allProfiles = new HashSet<>();
        Set<CompactionContext> allContexts = new HashSet<>();
        compactionTriggerConfigs.forEach(ctc -> allProfiles.addAll(ctc.getCompactionProfiles()));
        compactionTriggerConfigs.forEach(ctc -> allContexts.addAll(ctc.getCompactionContexts()));
        return builder.build();
    }

    /**
     * Saves config resource file in memory
     * @param resourceName
     */
    public void addResource(String resourceName) {
        log.info("Adding resource {}", resourceName);
        resourceNames.add(resourceName);
    }

    /**
     * Takes a list of config resourceNames and stores it in memory
     * @param resourceNames
     */
    public void addResources(String[] resourceNames) {
        for(String file: resourceNames) {
            this.addResource(file);
        }
    }

    /**
     * Creates singleton instance of Configuration instance
     * @return {CompactionTriggerConfig}
     * @throws ConfigurationException
     */
    public CompactionTriggerConfig getConfig() throws ConfigurationException {
        this.init();
        if (config == null) {
            throw new ConfigurationException("Compaction Trigger Config not initialized!! Check if config file passed correctly");
        }
        return config;
    }
}
