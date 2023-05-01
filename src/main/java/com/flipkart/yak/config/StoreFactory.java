package com.flipkart.yak.config;

import com.flipkart.yak.config.k8s.*;
import com.flipkart.yak.config.loader.AbstractConfigLoader;
import com.flipkart.yak.config.loader.AbstractConfigWriter;
import com.flipkart.yak.config.zkstore.*;
import com.flipkart.yak.interfaces.Factory;
import com.flipkart.yak.rest.AppConfig;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.Objects;

/**
 * Produces {@link Factory}
 */
@Slf4j
public class StoreFactory {

    private StoreFactory() {}

    private static Factory storeFactory;

    public static class StoreFactoryBuilder {
        private static AppConfig appConfig;

        public StoreFactoryBuilder withConfig(AppConfig config) {
            if( appConfig!= null) {
                throw new IllegalStateException( "AppConfig is already initialised");
            }
            appConfig = config;
            return this;
        }
        public Factory getFactory() throws Exception {
            if( appConfig==null){
                throw new IllegalStateException("Config is not initialised, Factory can not be created");
            }
            if(storeFactory == null) {
                if(appConfig.getStore().equals("zk")) {
                    storeFactory = new ZKStoreFactory();
                    storeFactory.init(appConfig.getZkConfig());
                }
                if(appConfig.getStore().equals("k8")) {
                    storeFactory = new K8sStoreFactory();
                    storeFactory.init(appConfig.getK8sConfig());
                }
            }
            return storeFactory;
         }
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

    /**
     * Produces DAO for K8s based store.
     */
    static class K8sStoreFactory implements Factory<K8sConfig> {
        private static AbstractConfigLoader abstractConfigLoader;
        private static AbstractConfigWriter abstractConfigWriter;
        private static ConfigListener configListener;

        private static K8sConfig k8sConfig;

        @Override
        public void init(K8sConfig resource) throws Exception {
            k8sConfig=resource;
            K8sUtils.init(resource.getNamespace());
        }

        @Override
        public AbstractConfigLoader getLoader() {
            if (abstractConfigLoader != null) {
                return abstractConfigLoader;
            }
            abstractConfigLoader = new K8sConfigLoader();
            abstractConfigLoader.addResource(k8sConfig.getNamespace());
            return abstractConfigLoader;
        }

        @Override
        public AbstractConfigWriter getWriter() {
            if (abstractConfigWriter != null) {
                return abstractConfigWriter;
            }
            abstractConfigWriter = new K8sConfigWriter();
            return abstractConfigWriter;
        }

        @Override
        public ConfigListener getConfigListener() throws Exception {
            if(configListener == null) {
                AbstractConfigLoader configLoader = getLoader();
                CoreV1Api coreV1Api = K8sUtils.getApi();
                ApiClient client = K8sUtils.getClient();
                client.setConnectTimeout(k8sConfig.getConnectionTimeout());
                client.setReadTimeout(k8sConfig.getReadTimeout());
                configListener = new K8sConfigListener(configLoader,coreV1Api, client, k8sConfig.getNamespace());
            }
            return configListener;
        }
    }

}
