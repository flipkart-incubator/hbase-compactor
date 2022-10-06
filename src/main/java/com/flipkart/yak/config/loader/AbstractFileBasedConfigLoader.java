package com.flipkart.yak.config.loader;

import com.flipkart.yak.config.CompactionTriggerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;

@Slf4j
public abstract class AbstractFileBasedConfigLoader extends AbstractConfigLoader{

    abstract protected CompactionTriggerConfig loadConfigFromFile(File file) throws ParserConfigurationException, IOException;

    /**
     * Takes a fileName and relies specific implementation to load and prepare a {CompactionTriggerConfig} object
     * @param fileName file that contains config, can be any implementation
     * @return prepared {CompactionTriggerConfig}
     * @throws ConfigurationException
     */
    protected CompactionTriggerConfig loadConfig(String fileName) throws ConfigurationException {
        File resourceFile = new File(fileName);
        if (!resourceFile.canRead()) {
            throw new ConfigurationException("Can not read file " + fileName);
        }

        if (!resourceFile.exists()) {
            throw new ConfigurationException("Can not read file " + fileName);
        }

        CompactionTriggerConfig config = null;
        try {
            config = this.loadConfigFromFile(resourceFile);
        } catch (ParserConfigurationException | IOException e) {
            log.error(e.getMessage());
        }
        return config;
    }
}
