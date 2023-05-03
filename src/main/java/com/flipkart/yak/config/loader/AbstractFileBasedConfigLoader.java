package com.flipkart.yak.config.loader;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;

import java.io.File;
import java.net.URL;

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
        URL resourceFile = Thread.currentThread().getContextClassLoader().getResource(fileName);
        if (resourceFile == null) {
            throw new ConfigurationException("Can not read file " + fileName);
        }
        File file = new File(resourceFile.getFile());
        if (!file.exists()) {
            throw new ConfigurationException("Can not read file " + fileName);
        }
        return file;
    }

    @Override
    protected void close(File file) {

    }

    public abstract void loadDefaults();

}
