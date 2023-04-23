package com.flipkart.yak.config.zkstore;


import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfileConfig;
import com.flipkart.yak.config.loader.AbstractConfigLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public class ZKConfigLoader extends AbstractConfigLoader<CuratorFramework> {
    private boolean readOnly;

    public ZKConfigLoader(boolean readOnly) {
        super();
        this.readOnly = readOnly;
    }

    @Override
    protected CuratorFramework preCheckAndLoad(String resourceName) throws ConfigurationException {
        CuratorFramework zk = ZKConnectionFactory.createZKConnector(resourceName, this.readOnly);
        ZKConnectionFactory.createBasePaths();
        return zk;
    }

    @Override
    public List<CompactionProfileConfig> getProfiles(CuratorFramework zooKeeper) throws ConfigurationException {
        List<String> profiles = new ArrayList<>();
        List<CompactionProfileConfig> profileConfigs= new ArrayList<>();
        try {
            log.info("getting data from {}", ZKDataUtil.getProfileBasePath());
            profiles = zooKeeper.getChildren().forPath(ZKDataUtil.getProfileBasePath());
            log.info("got {} profiles", profiles.size());
            profiles.forEach( path -> {
                try {
                    byte[] data =  zooKeeper.getData().forPath(ZKPaths.makePath(ZKDataUtil.getProfileBasePath(),path));
                    profileConfigs.add(ZKDataUtil.getProfileFromSerializedConfig(data));
                } catch (Exception e) {
                    log.error("could not get data from zookeeper path {}: {}", path, e.getMessage());
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            log.error("Could not load zookeeper data: {}", e.getMessage());
            throw new ConfigurationException(e);
        }
        return profileConfigs;
    }

    @Override
    public List<CompactionContext> getCompactionContexts(CuratorFramework zooKeeper) throws ConfigurationException {
        List<String> contextsData = new ArrayList<>();
        List<CompactionContext> contexts= new ArrayList<>();
        try {
            log.info("getting data from {}", ZKDataUtil.getContextBasePath());
            contextsData = zooKeeper.getChildren().forPath(ZKDataUtil.getContextBasePath());
            log.info("got {} contexts", contextsData.size());
            contextsData.forEach( path -> {
                try {
                    byte[] data =  zooKeeper.getData().forPath(ZKPaths.makePath(ZKDataUtil.getContextBasePath(),path));
                    contexts.add(ZKDataUtil.getContextFromSerializedConfig(data));
                } catch (Exception  e) {
                    log.error("could not get data from zookeeper path {}: {}", path, e.getMessage());
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            log.error("Could not load zookeeper data: {}", e.getMessage());
            throw new ConfigurationException(e);
        }
        return contexts;
    }

    @Override
    protected void close(CuratorFramework resourceType) {
        try {
            log.info("closing zookeeper connection!");
            resourceType.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
