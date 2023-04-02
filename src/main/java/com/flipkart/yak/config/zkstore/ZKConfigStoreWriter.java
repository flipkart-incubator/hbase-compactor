package com.flipkart.yak.config.zkstore;

import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfileConfig;
import com.flipkart.yak.config.loader.AbstractConfigWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;


@Slf4j
public class ZKConfigStoreWriter extends AbstractConfigWriter<ZooKeeper> {

    @Override
    public ZooKeeper init(String resourceName) throws ConfigurationException {
        try {
            return ZKConnectionFactory.createZKConnector(resourceName, false);
        } catch (ConfigurationException e) {
            log.error("Could not create connection : {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public boolean storeProfile(ZooKeeper zooKeeper, CompactionProfileConfig compactionProfileConfig) {
        byte[] data = ZKDataUtil.getSerializedProfile(compactionProfileConfig);
        log.debug("writing to zk {}", data);
        try {
            zooKeeper.setData(ZKDataUtil.getProfilePath(compactionProfileConfig.getID()), data, 1);
            return true;
        } catch (KeeperException | InterruptedException e) {
            log.error("Could not write to store: {}", e.getMessage());
        }
        return false;
    }

    @Override
    public boolean storeContext(ZooKeeper zooKeeper, CompactionContext compactionContext) {
        byte[] data = ZKDataUtil.getSerializedContext(compactionContext);
        log.debug("writing to zk {}", data);
        try {
            zooKeeper.setData(ZKDataUtil.getContextPath(compactionContext), data, 1);
            return true;
        } catch (KeeperException | InterruptedException e) {
            log.error("Could not write to store: {}", e.getMessage());
        }
        return false;
    }
}
