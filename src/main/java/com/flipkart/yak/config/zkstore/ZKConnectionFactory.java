package com.flipkart.yak.config.zkstore;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

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
}
