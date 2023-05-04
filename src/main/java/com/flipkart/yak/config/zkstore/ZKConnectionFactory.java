package com.flipkart.yak.config.zkstore;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;


@Slf4j
public class ZKConnectionFactory {
    private static CuratorFramework zooKeeper;

    //TODO: Should be part of zkConfig
    private static  RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);


    public static CuratorFramework createZKConnector(String hosts, Boolean readOnly) throws ConfigurationException {
        if(zooKeeper != null) {
            return zooKeeper;
        }
       zooKeeper = CuratorFrameworkFactory.newClient(hosts,retryPolicy);
       zooKeeper.start();
       return zooKeeper;
    }

    /**
     * every initialisation should be calling this method once, this checks if the Base Construct is present or not.
     * If not present, it creates. Any ACL implementation should be part of this method only.
     * @return true base construct is fine
     */
    public static boolean createBasePaths() {
        log.info("creating base path");
        if( zooKeeper != null) {
            try {
                if(zooKeeper.checkExists().forPath(ZKDataUtil.getBasePath()) == null) {
                    log.info("{} Does not exists, creating.",ZKDataUtil.getBasePath());
                    zooKeeper.create().forPath(ZKDataUtil.getBasePath());
                }
                if(zooKeeper.checkExists().forPath(ZKDataUtil.getContextBasePath()) == null) {
                    log.info("{} Does not exists, creating.",ZKDataUtil.getContextBasePath());
                    zooKeeper.create().forPath(ZKDataUtil.getContextBasePath());
                }
                if(zooKeeper.checkExists().forPath(ZKDataUtil.getProfileBasePath()) == null) {
                    log.info("{} Does not exists, creating.", ZKDataUtil.getProfileBasePath());
                    zooKeeper.create().forPath(ZKDataUtil.getProfileBasePath());
                }
            } catch (Exception e) {
                log.error("Could not create path: {}", e.getMessage());
                return false;
            }
            return true;
        }
        return  false;
    }


}
