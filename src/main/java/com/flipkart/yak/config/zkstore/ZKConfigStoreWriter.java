package com.flipkart.yak.config.zkstore;

import com.flipkart.yak.commons.ScheduleUtils;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfileConfig;
import com.flipkart.yak.config.loader.AbstractConfigWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public class ZKConfigStoreWriter extends AbstractConfigWriter<CuratorFramework> {

    @Override
    public CuratorFramework init(String resourceName) throws ConfigurationException {
        try {
            CuratorFramework zooKeeper = ZKConnectionFactory.createZKConnector(resourceName, false);
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
    public boolean storeProfile(CuratorFramework zooKeeper, CompactionProfileConfig compactionProfileConfig) {
        byte[] data = ZKDataUtil.getSerializedProfile(compactionProfileConfig);
        log.debug("writing to zk {}", data);
        try {
            String profilePath = ZKDataUtil.getProfilePath(compactionProfileConfig.getID());
            Stat checkIfExists = zooKeeper.checkExists().forPath(profilePath);
            if(checkIfExists == null) {
                log.info("{} : Profile does not exists", ZKDataUtil.getProfilePath(compactionProfileConfig.getID()));
                zooKeeper.create().withMode(CreateMode.PERSISTENT).forPath(profilePath, null);
            }
            int version = zooKeeper.setData().forPath(profilePath, data).getVersion();
            log.info("Written {} version for path {} with data {}", version, profilePath, data);
            return true;
        } catch (Exception e) {
            log.error("Could not write to store: {}", e.getMessage());
        }
        return false;
    }

    @Override
    public boolean storeContext(CuratorFramework zooKeeper, CompactionContext compactionContext) {
        byte[] data = ZKDataUtil.getSerializedContext(compactionContext);
        if(data == null) {
            return false;
        }
        log.debug("writing to zk {}", data);
        try {
            String contextPath = ZKDataUtil.getContextPath(compactionContext);
            Stat checkIfExists = zooKeeper.checkExists().forPath(contextPath);
            if( checkIfExists == null) {
                log.info("{} : Profile does not exists", contextPath);
                zooKeeper.create().withMode(CreateMode.PERSISTENT).forPath(contextPath, null);
            }
            int version = zooKeeper.setData().forPath(contextPath, data).getVersion();
            log.info("Written {} version for path {} with data {}", version, contextPath, data);
            return true;
        } catch (Exception e) {
            log.error("Could not write to store: {}", e.getMessage());
        }
        return false;
    }

    @Override
    public boolean deleteContext(CuratorFramework zooKeeper, CompactionContext compactionContext) {
        byte[] data = ZKDataUtil.getSerializedContext(compactionContext);
        if(data == null) {
            return false;
        }
        log.debug("deleting from zk {}", data);
        try {
            String contextPath = ZKDataUtil.getContextPath(compactionContext);
            Stat checkIfExists = zooKeeper.checkExists().forPath(contextPath);
            if( checkIfExists == null) {
                log.info("{} : Context does not exists", contextPath);
                return true;
            }
            zooKeeper.delete().forPath(contextPath);
            log.info("Deleted version for path {} ", contextPath);
            return true;
        } catch (Exception e) {
            log.error("Could not write to store: {}", e.getMessage());
        }
        return false;
    }

    @Override
    public boolean deleteAllStaleContexts(CuratorFramework zooKeeper) {
        List<String> contextsData = new ArrayList<>();
        try {
            contextsData = zooKeeper.getChildren().forPath(ZKDataUtil.getContextBasePath());
            contextsData.forEach( path -> {
                try {
                    byte[] data =  zooKeeper.getData().forPath(ZKPaths.makePath(ZKDataUtil.getContextBasePath(),path));
                    CompactionContext compactionContext = ZKDataUtil.getContextFromSerializedConfig(data);
                    if(compactionContext.getCompactionSchedule().isPrompt() && ScheduleUtils.hasTimedOut(compactionContext.getCompactionSchedule().getPromptCompactionLifespan(), Instant.now())) {
                        log.info("Deleting context {}",compactionContext.toString());
                        deleteContext(zooKeeper, compactionContext);
                    }
                } catch (Exception  e) {
                    log.error("could not get data from zookeeper path {}: {}", path, e.getMessage());
                    throw new RuntimeException(e);
                }
            });
            return true;
        } catch (Exception e) {
            log.error("Could not load zookeeper data: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
