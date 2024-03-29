package com.flipkart.yak.commons;


import com.flipkart.yak.config.CompactionProfileConfig;
import com.flipkart.yak.config.SerializedConfigurable;
import com.flipkart.yak.interfaces.PolicyAggregator;
import com.flipkart.yak.interfaces.ProfileInventory;
import com.flipkart.yak.interfaces.RegionSelectionPolicy;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.util.ReflectionUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class SimpleProfileInventory implements ProfileInventory  {

    /**
     * Multiple threads, primarily form Listener interface can access this and update concurrently. Hence, it is mandatory
     * to be {@link ConcurrentHashMap}.
     */
    final Map<String, CompactionProfile> profileInventory = new ConcurrentHashMap<>();

    @Override
    public CompactionProfile loadProfileFromName(CompactionProfileConfig compactionProfileConfig) throws ClassNotFoundException {
        log.info("loading {}", compactionProfileConfig.getID());
        PolicyAggregator aggregator = (PolicyAggregator) ReflectionUtils.newInstance(Class.forName(compactionProfileConfig.getAggregator().getFirst()));
        Set<RegionSelectionPolicy> regionSelectionPolicies = new HashSet<>();
        for(SerializedConfigurable serializedConfigurable : compactionProfileConfig.getPolicies()) {
            RegionSelectionPolicy regionSelectionPolicy = (RegionSelectionPolicy)ReflectionUtils.newInstance(Class.forName(serializedConfigurable.getFirst()));
            regionSelectionPolicy.setFromConfig(serializedConfigurable.getSecond());
            regionSelectionPolicies.add(regionSelectionPolicy);
        }
        log.info("Loaded {}", regionSelectionPolicies.size());
        aggregator.setFromConfig(compactionProfileConfig.getAggregator().getSecond());
        return new CompactionProfile(compactionProfileConfig.getID(), regionSelectionPolicies, aggregator);
    }

    @Override
    public void add(CompactionProfile compactionProfile) {
        log.debug("adding {}", compactionProfile);
        profileInventory.put(compactionProfile.getID(), compactionProfile);
    }

    @Override
    public void handleExceptionWithoutThrowing(ClassNotFoundException e) {
        log.error("Class Not Found: {}", e.getMessage());
    }

    @Override
    public void reset() {
        profileInventory.clear();
    }


    @Override
    public CompactionProfile get(String key) {
        return profileInventory.get(key);
    }
}
