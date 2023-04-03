package com.flipkart.yak.config.zkstore;


import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfileConfig;
import com.flipkart.yak.config.loader.AbstractConfigLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public class ZKConfigLoader extends AbstractConfigLoader<ZooKeeper> {

    @Override
    protected ZooKeeper preCheckAndLoad(String resourceName) throws ConfigurationException {
        return ZKConnectionFactory.createZKConnector(resourceName, true);
    }

    @Override
    public List<CompactionProfileConfig> getProfiles(ZooKeeper zooKeeper) throws ConfigurationException {
        List<String> profiles = new ArrayList<>();
        List<CompactionProfileConfig> profileConfigs= new ArrayList<>();
        try {
            log.info("getting data from {}", ZKDataUtil.BASE_PATH + ZKDataUtil.PROFILE_PATH);
            profiles = zooKeeper.getChildren(ZKDataUtil.BASE_PATH + ZKDataUtil.PROFILE_PATH, false);
            log.info("got {} profiles", profiles.size());
            profiles.forEach( path -> {
                try {
                    byte[] data =  zooKeeper.getData(ZKDataUtil.BASE_PATH + ZKDataUtil.PROFILE_PATH + "/"+path, false, null);
                    profileConfigs.add(ZKDataUtil.getProfileFromSerializedConfig(data));
                } catch (KeeperException | InterruptedException | IOException e) {
                    log.error("could not get data from zookeeper path {}: {}", path, e.getMessage());
                    throw new RuntimeException(e);
                }
            });
        } catch (KeeperException | InterruptedException | RuntimeException e) {
            log.error("Could not load zookeeper data: {}", e.getMessage());
            throw new ConfigurationException(e);
        }
        return profileConfigs;
    }

    @Override
    public List<CompactionContext> getCompactionContexts(ZooKeeper zooKeeper) throws ConfigurationException {
        List<String> contextsData = new ArrayList<>();
        List<CompactionContext> contexts= new ArrayList<>();
        try {
            log.info("getting data from {}", ZKDataUtil.BASE_PATH + ZKDataUtil.CONTEXT_PATH);
            contextsData = zooKeeper.getChildren(ZKDataUtil.BASE_PATH + ZKDataUtil.CONTEXT_PATH, false);
            log.info("got {} contexts", contextsData.size());
            contextsData.forEach( path -> {
                try {
                    byte[] data =  zooKeeper.getData(ZKDataUtil.BASE_PATH + ZKDataUtil.CONTEXT_PATH + "/"+path, false, null);
                    contexts.add(ZKDataUtil.getContextFromSerializedConfig(data));
                } catch (KeeperException | InterruptedException |IOException  e) {
                    log.error("could not get data from zookeeper path {}: {}", path, e.getMessage());
                    throw new RuntimeException(e);
                }
            });
        } catch (KeeperException | InterruptedException | RuntimeException e) {
            log.error("Could not load zookeeper data: {}", e.getMessage());
            throw new ConfigurationException(e);
        }
        return contexts;
    }

    @Override
    protected void close(ZooKeeper resourceType) {
        try {
            resourceType.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
