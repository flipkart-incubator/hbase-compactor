package com.flipkart.yak.config;

import com.flipkart.yak.config.loader.AbstractConfigLoader;
import com.flipkart.yak.config.loader.AbstractConfigWriter;
import com.flipkart.yak.config.zkstore.*;
import com.flipkart.yak.interfaces.Factory;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

/**
 * Produces {@link Factory}
 */
@Slf4j
public class StoreFactory {

    private StoreFactory() {}

    private static Factory storeFactory;


    public static Factory getInstance() {
        if (storeFactory == null) {
            storeFactory = new ZKStoreFactory();
        }
        return storeFactory;
    }

    /**
     * Produces DAOs for Zookeeper Based Store
     */
    static class ZKStoreFactory implements Factory<ZKConfig> {
        private static AbstractConfigLoader abstractConfigLoader;
        private static AbstractConfigWriter abstractConfigWriter;
        private static ConfigListener configListener;
        private static CuratorFramework zookeeper;
        private static ZKConfig zkConfig;

        @Override
        public void init(ZKConfig resource) throws Exception {
            zkConfig = resource;
            zookeeper = ZKConnectionFactory.createZKConnector(zkConfig.getConnectionString(), zkConfig.isReadOnly());
            getLoader();
            abstractConfigLoader.addResource(zkConfig.getConnectionString());
            getWriter();
            getConfigListener();
        }

        @Override
        public AbstractConfigLoader getLoader() {
            if ( abstractConfigLoader == null) {
                abstractConfigLoader = new ZKConfigLoader(false);
            }
            return abstractConfigLoader;
        }

        @Override
        public AbstractConfigWriter getWriter() {
            if (abstractConfigWriter == null) {
                abstractConfigWriter = new ZKConfigStoreWriter();
            }
            return abstractConfigWriter;
        }

        @Override
        public ConfigListener getConfigListener() throws Exception {
            if (zookeeper == null) {
                throw new RuntimeException("resource not initialised. call init() before doing anything.");
            }
            if(configListener == null) {
                configListener = new ZkConfigListener(abstractConfigLoader, zookeeper);
            }
            return configListener;
        }
    }

}
