package com.flipkart.yak.config;

import com.flipkart.yak.config.loader.AbstractConfigLoader;
import com.flipkart.yak.config.xml.XMLConfigLoader;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.jupiter.api.Test;

public class XMLConfigLoderTest {

    @Test
    public void testBasicLoading() throws ConfigurationException {
        AbstractConfigLoader configLoader = new XMLConfigLoader();
        configLoader.addResource("src/test/resources/namespace-1-config.xml");
        configLoader.getConfig();
    }
}
