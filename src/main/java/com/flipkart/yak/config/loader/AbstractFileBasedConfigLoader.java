package com.flipkart.yak.config.loader;

import com.flipkart.yak.config.CompactionTriggerConfig;
import org.apache.commons.configuration.ConfigurationException;

import java.io.File;

public abstract class AbstractFileBasedConfigLoader extends AbstractConfigLoader{

    abstract protected CompactionTriggerConfig loadConfigFromFile(String file);

    /**
     * Takes a fileName and relies specific implementation to load and prepare a {CompactionTriggerConfig} object
     * @param fileName file that contains config, can be any implementation
     * @return prepared {CompactionTriggerConfig}
     * @throws ConfigurationException
     */
    protected CompactionTriggerConfig loadConfig(String fileName) throws ConfigurationException {
        File resourceFile = new File(fileName);
        if (resourceFile.canRead()) {
            throw new ConfigurationException("Can not read file " + fileName);
        }

        if (resourceFile.exists()) {
            throw new ConfigurationException("Can not read file " + fileName);
        }

        CompactionTriggerConfig config = this.loadConfigFromFile(fileName);
        return config;
    }
}
