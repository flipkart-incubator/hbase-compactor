package com.flipkart.yak.config;

import com.flipkart.yak.config.loader.AbstractConfigLoader;
import com.flipkart.yak.config.xml.XMLConfigLoader;
import org.apache.commons.configuration.ConfigurationException;

public class CompactionConfigManger {

    private static CompactionTriggerConfig compactionTriggerConfig;

    public static CompactionTriggerConfig get(String... args) throws ConfigurationException {
        if(compactionTriggerConfig == null) {
            AbstractConfigLoader configLoader = StoreFactory.getInstance().getLoader();
            compactionTriggerConfig = configLoader.getConfig();
        }
        return compactionTriggerConfig;
    }
}
