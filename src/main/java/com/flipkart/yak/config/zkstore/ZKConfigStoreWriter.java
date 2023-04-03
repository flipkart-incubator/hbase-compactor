package com.flipkart.yak.config.zkstore;

import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfileConfig;
import com.flipkart.yak.config.loader.AbstractConfigWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


@Slf4j
public class ZKConfigStoreWriter extends AbstractConfigWriter<ZooKeeper> {

    @Override
    public ZooKeeper init(String resourceName) throws ConfigurationException {
        try {
            ZooKeeper zooKeeper = ZKConnectionFactory.createZKConnector(resourceName, false);
            boolean initSuccess = ZKConnectionFactory.createBasePaths();
            if (!initSuccess) {
                log.error("could not create path for initialisation");
                throw new ConfigurationException("Zookeeper path could not be created");
            }
            return zooKeeper;
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
            String profilePath = ZKDataUtil.getProfilePath(compactionProfileConfig.getID());
            Stat checkIfExists = zooKeeper.exists(profilePath, null);
            if(checkIfExists == null) {
                log.info("{} : Profile does not exists", ZKDataUtil.getProfilePath(compactionProfileConfig.getID()));
                zooKeeper.create(profilePath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            int version = checkIfExists == null? 0: checkIfExists.getVersion();
            log.info("{} version {}", profilePath, version);
            zooKeeper.setData(profilePath, data, version);
            return true;
        } catch (KeeperException | InterruptedException e) {
            log.error("Could not write to store: {}", e.getMessage());
        }
        return false;
    }

    @Override
    public boolean storeContext(ZooKeeper zooKeeper, CompactionContext compactionContext) {
        byte[] data = ZKDataUtil.getSerializedContext(compactionContext);
        if(data == null) {
            return false;
        }
        log.debug("writing to zk {}", data);
        try {
            String contextPath = ZKDataUtil.getContextPath(compactionContext);
            Stat checkIfExists = zooKeeper.exists(contextPath, null);
            if( checkIfExists == null) {
                log.info("{} : Profile does not exists", contextPath);
                zooKeeper.create(contextPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            int version = checkIfExists == null? 0: checkIfExists.getVersion();
            log.info("{} version {}", contextPath, version);
            zooKeeper.setData(contextPath, data, version);
            return true;
        } catch (KeeperException | InterruptedException e) {
            log.error("Could not write to store: {}", e.getMessage());
        }
        return false;
    }
}
