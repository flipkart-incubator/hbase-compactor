package com.flipkart.yak.config.zkstore;

import com.flipkart.yak.config.CompactionProfileConfig;
import com.flipkart.yak.config.CompactionTriggerConfig;
import com.flipkart.yak.config.ConfigListener;
import com.flipkart.yak.config.loader.AbstractConfigLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.Semaphore;

@Slf4j
public class ZkConfigListener extends ConfigListener implements PathChildrenCacheListener {

    AbstractConfigLoader<ZooKeeper> configLoader;
    CuratorFramework zookeeper;
    Semaphore semaphore = new Semaphore(-1);

    public ZkConfigListener(AbstractConfigLoader configLoader, CuratorFramework zookeeper) {
        super(configLoader);
        this.zookeeper = zookeeper;
    }
    @Override
    public void onChange() throws ConfigurationException {
        super.onChange();
    }

    private void setup() throws Exception {
        PathChildrenCache context = new PathChildrenCache(zookeeper, ZKDataUtil.getContextBasePath(), true);
        PathChildrenCache profile = new PathChildrenCache(zookeeper, ZKDataUtil.getProfileBasePath(), true);
        context.start();
        profile.start();
        context.getListenable().addListener(this);
        profile.getListenable().addListener(this);
        log.info("setup listener completed");
        semaphore.acquire();
    }

    @Override
    public void listen() throws ConfigurationException {
        try {
            log.info("registering listener!  starting to watch event change");
            this.setup();
        } catch (ConfigurationException e) {
            log.error("Could not load configuration: {}",e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Could not load configuration: {}",e.getMessage());
            throw new ConfigurationException(e);
        }
    }

    @Override
    public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
        log.info("Cache Event detected {}", event.getType());
        switch (event.getType()) {
            case INITIALIZED:
                semaphore.release();
                break;
            case CHILD_ADDED:
            case CHILD_UPDATED:
            case CHILD_REMOVED: {
                this.onChange();
                log.debug("reload completed after cache event");
                break;
            }
            default:
                log.info("Un-handled event {}", event.getType().name());
        }
    }
}
