package com.flipkart.yak.config.zkstore;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ZKConnectionFactory {

    private static final CountDownLatch connectionLatch = new CountDownLatch(1);
    private static ZooKeeper zooKeeper;

    private static Watcher watcher;

    public static Watcher getWatcher() {
        if(watcher!= null) {
            return watcher;
        }
        watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    connectionLatch.countDown();
                }
            }
        };
        return watcher;
    }

    public static ZooKeeper createZKConnector(String hosts, Boolean readOnly) throws ConfigurationException {
        if (zooKeeper != null) {
            return zooKeeper;
        }
        try {
            zooKeeper = new ZooKeeper(hosts, ZKDataUtil.DEFAULT_SESSION_TIMEOUT, getWatcher() , readOnly);
            connectionLatch.await();
        } catch (IOException | InterruptedException  e) {
            log.error("Could not connect to zookeeper: {}", e.getMessage());
            throw new ConfigurationException(e);
        }
        return zooKeeper;
    }

    public static boolean createBasePaths() {
        log.info("creating base path");
        if( zooKeeper != null) {
            try {
                if(zooKeeper.exists(ZKDataUtil.BASE_PATH, null) == null) {
                    log.info("{} Does not exists, creating.", ZKDataUtil.BASE_PATH);
                    zooKeeper.create(ZKDataUtil.BASE_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                if(zooKeeper.exists(ZKDataUtil.BASE_PATH+ZKDataUtil.CONTEXT_PATH, null) == null) {
                    log.info("{} Does not exists, creating.", ZKDataUtil.BASE_PATH+ZKDataUtil.CONTEXT_PATH);
                    zooKeeper.create(ZKDataUtil.BASE_PATH+ZKDataUtil.CONTEXT_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }
                if(zooKeeper.exists(ZKDataUtil.BASE_PATH+ZKDataUtil.PROFILE_PATH, null) == null) {
                    log.info("{} Does not exists, creating.", ZKDataUtil.BASE_PATH+ZKDataUtil.PROFILE_PATH);
                    zooKeeper.create(ZKDataUtil.BASE_PATH+ZKDataUtil.PROFILE_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }

            } catch (KeeperException | InterruptedException e) {
                log.error("Could not create path: {}", e.getMessage());
                return false;
            }
            return true;
        }
        return  false;
    }
}
