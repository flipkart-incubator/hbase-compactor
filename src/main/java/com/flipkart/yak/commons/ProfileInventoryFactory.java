package com.flipkart.yak.commons;

import com.flipkart.yak.config.CompactionConfigManger;
import com.flipkart.yak.config.CompactionTriggerConfig;
import com.flipkart.yak.interfaces.ProfileInventory;
import org.apache.commons.configuration.ConfigurationException;

public class ProfileInventoryFactory {
    private static ProfileInventory profileInventory;
    private ProfileInventoryFactory() {};

    public static ProfileInventory getProfileInventory() throws ConfigurationException {
        if (profileInventory == null) {
            profileInventory = new SimpleProfileInventory();
            CompactionTriggerConfig config = CompactionConfigManger.get();
            profileInventory.reload(config);
        }
        return profileInventory;
    }
}
