package com.flipkart.yak.core;

import com.flipkart.yak.commons.ConnectionInventory;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionTriggerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


@Slf4j
public class JobSubmitter {

    CompactionTriggerConfig compactionTriggerConfig;
    ExecutorService executorService;
    final List<Future> compactors;


    public JobSubmitter(CompactionTriggerConfig compactionTriggerConfig) throws CompactionRuntimeException {
        this.compactionTriggerConfig = compactionTriggerConfig;
        log.info("Loaded total {} contexts", this.compactionTriggerConfig.getCompactionContexts().size());
        log.info("Loaded total {} profile configs", this.compactionTriggerConfig.getCompactionProfileConfigs().size());
        this.compactors = new ArrayList<>(this.compactionTriggerConfig.getCompactionContexts().size());
        if (this.compactionTriggerConfig.getCompactionContexts().size() < 1 || this.compactionTriggerConfig.getCompactionProfileConfigs().size() < 1) {
            throw new CompactionRuntimeException("Minimum number of Context or Profiles are not there");
        }
        this.executorService = Executors.newFixedThreadPool(this.compactionTriggerConfig.getCompactionContexts().size());
    }

    public void start() {
        MonitorService.start();
        this.preLoadConnections();

        for (CompactionContext compactionContext : this.compactionTriggerConfig.getCompactionContexts()) {
            ThreadedCompactionJob threadedCompactionJob = new ThreadedCompactionJob();
            try {
                threadedCompactionJob.init(compactionContext);
            } catch (ConfigurationException e) {
                log.error("Ignoring Context {} error {}", compactionContext, e.getMessage());
            }
            compactors.add(this.executorService.submit(threadedCompactionJob));
            log.info("submitted for {}:{}", compactionContext.getNameSpace(), compactionContext.getTableName());
        }
        this.executorService.shutdown();
    }

    private void preLoadConnections() {
        ConnectionInventory connectionInventory = ConnectionInventory.getInstance();
        for(CompactionContext compactionContext : compactionTriggerConfig.getCompactionContexts()) {
            try {
                log.debug("trying to create connection with {}", compactionContext.getClusterID());
                Connection connection = ConnectionFactory.createConnection(this.getHbaseConfig(compactionContext));
                connectionInventory.putIfAbsent(compactionContext.getClusterID(), connection);
            } catch (IOException e) {
                log.error("Exception while bootstrapping connection with {} - error: {}", compactionContext.getClusterID(), e.getMessage());
            }
        }
    }

    public Configuration getHbaseConfig(CompactionContext context) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.clear();
        log.debug("creating hbase config with {}", context.getClusterID());
        configuration.set("hbase.zookeeper.quorum", ConnectionInventory.getZookeeperQuorum(context.getClusterID()));
        configuration.set("zookeeper.znode.parent", ConnectionInventory.getParentFromID(context.getClusterID()));
        return configuration;
    }


}
