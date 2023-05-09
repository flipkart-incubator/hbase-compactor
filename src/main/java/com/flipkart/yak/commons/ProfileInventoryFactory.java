package com.flipkart.yak.commons;

import com.flipkart.yak.interfaces.ProfileInventory;
import org.apache.commons.configuration.ConfigurationException;

public class ProfileInventoryFactory {
    private static ProfileInventory profileInventory;
    private ProfileInventoryFactory() {};

    public static ProfileInventory getProfileInventory() throws ConfigurationException {
        if (profileInventory == null) {
            profileInventory = new SimpleProfileInventory();
        }
        return profileInventory;
    }
}
