package com.flipkart.yak.config.loader;

import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfileConfig;
import com.flipkart.yak.config.CompactionTriggerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;

import java.util.*;

@Slf4j
public abstract class AbstractConfigLoader <Resource> {
    CompactionTriggerConfig config = null;
    List<String> resourceNames = new ArrayList<>();


    protected abstract Resource preCheckAndLoad(String resourceName) throws ConfigurationException;
    public abstract List<CompactionProfileConfig> getProfiles(Resource resource) throws ConfigurationException;
    public abstract List<CompactionContext> getCompactionContexts(Resource resource) throws ConfigurationException;
    protected abstract void close(Resource resourceType);

    /**
     * initializes the config, ignores if issue in loading config
     */
    private void init() {
        if (config == null) {
            List<CompactionTriggerConfig> compactionTriggerConfigs = new ArrayList<>();
            log.info("Number of resources: {}", this.resourceNames.size());
            this.resourceNames.forEach(resource -> {
                Resource r = null;
                try {
                    r = this.preCheckAndLoad(resource);
                    List<CompactionProfileConfig> profiles = this.getProfiles(r);
                    List<CompactionContext> contexts = this.getCompactionContexts(r);
                    compactionTriggerConfigs.add(this.buildCompactorTriggerConfig(profiles, contexts));
                } catch (ConfigurationException e) {
                    log.warn("Found issue with {} while loading.. ignoring this resource: {}", resource, e.getMessage());
                }

            });
            this.config = this.mergeConfig(compactionTriggerConfigs);
        }
    }

    private CompactionTriggerConfig buildCompactorTriggerConfig(List<CompactionProfileConfig> profiles, List<CompactionContext> contexts) {
        CompactionTriggerConfig.Builder builder = new CompactionTriggerConfig.Builder();
        profiles.forEach(builder::withCompactionProfile);
        contexts.forEach(builder::withCompactionContext);
        return builder.build();
    }


    /**
     * Create sinle {@link CompactionTriggerConfig} from multiple Config. This is useful when multiple tenant wants to
     * keep separate CompactionTriggerConfig, this utility combines all of them into one.
     * @param compactionTriggerConfigs List of CompactionTriggerConfig collected from all resources.
     * @return Combined CompactionTriggerConfig
     */
    private CompactionTriggerConfig mergeConfig(List<CompactionTriggerConfig> compactionTriggerConfigs) {
        CompactionTriggerConfig.Builder builder = new CompactionTriggerConfig.Builder();
        Set<CompactionProfileConfig> allProfiles = new HashSet<>();
        Set<CompactionContext> allContexts = new HashSet<>();
        compactionTriggerConfigs.forEach(ctc -> allProfiles.addAll(ctc.getCompactionProfileConfigs()));
        compactionTriggerConfigs.forEach(ctc -> allContexts.addAll(ctc.getCompactionContexts()));
        builder.withCompactionProfiles(allProfiles);
        builder.withCompactionContexts(allContexts);
        return builder.build();
    }

    /**
     * Saves config resource file in memory, this method needs to be called to add Store config
     * @param resourceName
     */
    public void addResource(String resourceName) {
        log.info("Adding resource {}", resourceName);
        resourceNames.add(resourceName);
    }


    /**
     * Utility in case default classes needs to escaped
     */
    public void clearDefaultResources() {
        resourceNames.clear();
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

    /**
     * Clears in-memory config and loads the new one. Should be called before every restart.
     * @return Newly loaded {@link CompactionTriggerConfig}
     * @throws ConfigurationException
     */
    public CompactionTriggerConfig clearGetConfig() throws ConfigurationException {
        config = null;
        return  getConfig();
    }
}
