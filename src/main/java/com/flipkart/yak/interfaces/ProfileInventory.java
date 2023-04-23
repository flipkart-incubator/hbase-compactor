package com.flipkart.yak.interfaces;

import com.flipkart.yak.commons.CompactionProfile;
import com.flipkart.yak.config.CompactionProfileConfig;
import com.flipkart.yak.config.CompactionTriggerConfig;


public interface ProfileInventory {

    CompactionProfile loadProfileFromName(CompactionProfileConfig compactionProfileConfig) throws ClassNotFoundException;
    void add(CompactionProfile compactionProfile);
    void handleExceptionWithoutThrowing(ClassNotFoundException e);
    void reset();
    CompactionProfile get(String key);

    default void reload(CompactionTriggerConfig compactionTriggerConfig) {
        this.reset();
        for(CompactionProfileConfig compactionProfileConfig : compactionTriggerConfig.getCompactionProfileConfigs()) {
            try {
                this.add(this.loadProfileFromName(compactionProfileConfig));
            } catch (ClassNotFoundException e) {
                handleExceptionWithoutThrowing(e);
            }
        }
    }
}
