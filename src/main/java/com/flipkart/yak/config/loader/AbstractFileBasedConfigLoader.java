package com.flipkart.yak.config.loader;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;

import java.io.File;

@Slf4j
public abstract class AbstractFileBasedConfigLoader extends AbstractConfigLoader<File>{

    protected AbstractFileBasedConfigLoader() {
        this.loadDefaults();
    }

    /**
     * Takes a fileName and relies specific implementation to load and prepare a {CompactionTriggerConfig} object
     * @param fileName file that contains config, can be any implementation
     * @return prepared {CompactionTriggerConfig}
     * @throws ConfigurationException
     */
    @Override
    protected File preCheckAndLoad(String fileName) throws ConfigurationException {
        File resourceFile = new File(fileName);
        if (!resourceFile.canRead()) {
            throw new ConfigurationException("Can not read file " + fileName);
        }

        if (!resourceFile.exists()) {
            throw new ConfigurationException("Can not read file " + fileName);
        }
        return resourceFile;
    }

    @Override
    protected void close(File file) {

    }

    public abstract void loadDefaults();

}
