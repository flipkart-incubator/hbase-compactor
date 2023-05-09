package com.flipkart.yak.commons;

import com.flipkart.yak.config.CompactionProfileConfig;
import com.flipkart.yak.config.CompactionTriggerConfig;
import com.flipkart.yak.config.loader.AbstractConfigLoader;
import com.flipkart.yak.config.xml.XMLConfigLoader;
import com.flipkart.yak.interfaces.ProfileInventory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class ProfileInventoryTest {
    AbstractConfigLoader configLoader = null;

    @Before
    public void setup() {
        configLoader = new XMLConfigLoader();
        configLoader.clearDefaultResources();
    }

    @Test
    public void testSimpleLoading() {
        configLoader.addResource("namespace-1-config.xml");
        try {
            CompactionTriggerConfig compactionTriggerConfig = configLoader.getConfig();
            Set<CompactionProfileConfig> compactionProfiles = compactionTriggerConfig.getCompactionProfileConfigs();
            ProfileInventory profileInventory  = ProfileInventoryFactory.getProfileInventory();
            profileInventory.reload(compactionTriggerConfig);
            CompactionProfile profile = profileInventory.loadProfileFromName(compactionProfiles.iterator().next());
            assert profile.getAggregator().getClass().getName().equals("com.flipkart.yak.aggregator.SimpleUnionAggregator");
            assert compactionProfiles.size() == 1;
            assert profile.getID().equals("testID");
            assert profile.getPolicies().size() == 1;
            assert profile.getPolicies().iterator().next().getClass().getName().equals("com.flipkart.yak.policies.NaiveRegionSelectionPolicy");
        } catch (ConfigurationException | ClassNotFoundException e) {
            log.error(e.getMessage());
        }
    }

    @Test
    public void testMultiplePolicies() {
        configLoader.addResource("namespace-2-config.xml");
        try {
            CompactionTriggerConfig compactionTriggerConfig = configLoader.getConfig();
            Set<CompactionProfileConfig> compactionProfiles = compactionTriggerConfig.getCompactionProfileConfigs();
            ProfileInventory profileInventory  = ProfileInventoryFactory.getProfileInventory();
            profileInventory.reload(compactionTriggerConfig);
            CompactionProfile profile = profileInventory.loadProfileFromName(compactionProfiles.iterator().next());
            Set<String> profileNames = profile.getPolicies().stream().map(e -> e.getClass().getName()).collect(Collectors.toSet());
            assert profile.getAggregator().getClass().getName().equals("com.flipkart.yak.aggregator.ChainReportAggregator");
            assert compactionProfiles.size() == 1;
            assert profile.getID().equals("testID");
            assert profile.getPolicies().size() == 2;
            assert profileNames.contains("com.flipkart.yak.policies.NaiveRegionSelectionPolicy");
            assert profileNames.contains("com.flipkart.yak.policies.TimestampAwareSelectionPolicy");
        } catch (ConfigurationException | ClassNotFoundException e) {
            log.error(e.getMessage());
        }
    }

    @SneakyThrows
    @Test
    public void testNonExistingPolicies() {
        configLoader.addResource("namespace-3-config.xml");
        CompactionTriggerConfig compactionTriggerConfig = configLoader.getConfig();
        ProfileInventory profileInventory  = ProfileInventoryFactory.getProfileInventory();
        profileInventory.reload(compactionTriggerConfig);
        assert profileInventory.get("testID") == null;
        assert compactionTriggerConfig.getCompactionProfileConfigs().size() == 1;
    }
}
