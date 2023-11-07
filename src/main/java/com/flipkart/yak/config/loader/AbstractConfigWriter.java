package com.flipkart.yak.config.loader;

import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfileConfig;
import org.apache.commons.configuration.ConfigurationException;


public abstract class AbstractConfigWriter <Resource> {

    public abstract Resource init(String resourceName) throws ConfigurationException;
    public abstract boolean storeProfile(Resource resource, CompactionProfileConfig compactionProfileConfig);
    public abstract boolean storeContext(Resource resource, CompactionContext compactionContext);
    public abstract boolean deleteContext(Resource resource, CompactionContext compactionContext);
    public abstract boolean deleteStaleContexts(Resource resource);

}
