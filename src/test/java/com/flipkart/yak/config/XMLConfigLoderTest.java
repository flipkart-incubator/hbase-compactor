package com.flipkart.yak.config;

import com.flipkart.yak.config.loader.AbstractConfigLoader;
import com.flipkart.yak.config.xml.XMLConfigLoader;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.jupiter.api.Test;

class XMLConfigLoderTest {

    @Test
    void testBasicLoadingWithXML() throws ConfigurationException {
        AbstractConfigLoader configLoader = new XMLConfigLoader();
        configLoader.addResource("src/test/resources/namespace-1-config.xml");
        CompactionTriggerConfig config1 = configLoader.getConfig();
        CompactionTriggerConfig config2 = configLoader.getConfig();
        CompactionTriggerConfig compactionTriggerConfig = configLoader.getConfig();
        CompactionProfileConfig compactionProfileConfig = compactionTriggerConfig.compactionProfileConfigs.iterator().next();
        assert config1 == config2;
        assert compactionTriggerConfig.compactionProfileConfigs.size() == 1;
        assert compactionTriggerConfig.compactionContexts.size() == 2;
        assert compactionProfileConfig.getID().equals("testID");
        assert compactionProfileConfig.getAggregator().getFirst().equals("com.flipkart.yak.aggregator.SimpleUnionAggregator");
        assert compactionProfileConfig.getPolicies().size() == 1;
        assert compactionProfileConfig.getPolicies().iterator().next().getFirst().equals("com.flipkart.yak.policies.NaiveRegionSelectionPolicy");
    }

    @Test
    void testBasicLoadingWithXML2() throws ConfigurationException {
        AbstractConfigLoader configLoader = new XMLConfigLoader();
        configLoader.addResource("src/test/resources/namespace-2-config.xml");
        CompactionTriggerConfig config1 = configLoader.getConfig();
        CompactionTriggerConfig config2 = configLoader.getConfig();
        CompactionTriggerConfig compactionTriggerConfig = configLoader.getConfig();
        CompactionProfileConfig compactionProfileConfig = compactionTriggerConfig.compactionProfileConfigs.iterator().next();
        assert config1 == config2;
        assert compactionTriggerConfig.compactionProfileConfigs.size() == 1;
        assert config1.getCompactionContexts().size() == 1;
        assert config1.getCompactionContexts().iterator().next().getTableName().equals("preprod_compaction:table_1");
        assert compactionTriggerConfig.compactionContexts.size() == 1;
        assert compactionProfileConfig.getID().equals("base");
        assert compactionProfileConfig.getAggregator().getFirst().equals("com.flipkart.yak.aggregator.SimpleUnionAggregator");
        assert compactionProfileConfig.getPolicies().size() == 1;
        assert compactionProfileConfig.getPolicies().iterator().next().getFirst().equals("com.flipkart.yak.policies.NaiveRegionSelectionPolicy");
    }
}
