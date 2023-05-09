package com.flipkart.yak.core;

import com.flipkart.yak.commons.ConnectionInventory;
import com.flipkart.yak.commons.ProfileInventoryFactory;
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
import java.util.concurrent.*;

/**
 * Container for compaction tasks. Each task is ran inside of a {@link ThreadedCompactionJob}
 */
@Slf4j
public class JobSubmitter {

    private CompactionTriggerConfig compactionTriggerConfig;
    private ExecutorService executorService;
    private List<Future> compactors;


    public void init(CompactionTriggerConfig compactionTriggerConfig) throws CompactionRuntimeException {
        this.compactionTriggerConfig = compactionTriggerConfig;
        log.info("Loaded total {} contexts", this.compactionTriggerConfig.getCompactionContexts().size());
        log.info("Loaded total {} profile configs", this.compactionTriggerConfig.getCompactionProfileConfigs().size());
        this.compactors = new ArrayList<>(this.compactionTriggerConfig.getCompactionContexts().size());
        if (this.compactionTriggerConfig.getCompactionContexts().size() < 1 || this.compactionTriggerConfig.getCompactionProfileConfigs().size() < 1) {
            throw new CompactionRuntimeException("Minimum number of Context or Profiles are not there");
        }
        try {
            ProfileInventoryFactory.getProfileInventory().reload(this.compactionTriggerConfig);
        } catch (ConfigurationException e) {
            log.error("Could not reset inventory");
            throw new CompactionRuntimeException(e);
        }
        this.executorService = Executors.newFixedThreadPool(this.compactionTriggerConfig.getCompactionContexts().size());
    }

    public void start() {
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

    private void await() {
        if ( this.compactors != null) {
            for(Future compactionTask : compactors) {
                try {
                    if(!compactionTask.isDone() || !compactionTask.isCancelled()) {
                        compactionTask.get();
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.error("await for thread termination interrupted, this will be ignored and will be moved to next");
                }
                log.info("compaction task stop status: {}", compactionTask.isDone());
            }
        }
    }

    public void stop() {
        if(compactors == null) {
            log.info("not initiated yet, nothing to stop");
            return;
        }
        for(Future compactionTask : compactors) {
         compactionTask.cancel(true);
         log.info("awaiting compaction task to end");
        }
        this.await();
        if(executorService!=null) {
            executorService.shutdownNow();
        }
        ConnectionInventory.getInstance().forEach((K,V) -> {
            try {
                V.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        ConnectionInventory.getInstance().clear();
    }

    public void reload(CompactionTriggerConfig compactionTriggerConfig) throws CompactionRuntimeException {
        synchronized (this) {
            log.info("stopping all existing tasks");
            this.stop();
            log.info("initialising");
            this.init(compactionTriggerConfig);
            log.info("starting again");
            this.start();
            log.info("reload completed");
        }
    }

    private void preLoadConnections() {
        ConnectionInventory connectionInventory = ConnectionInventory.getInstance();
        for(CompactionContext compactionContext : compactionTriggerConfig.getCompactionContexts()) {
            try {
                log.debug("trying to create connection with {}", compactionContext.getClusterID());
                if(!connectionInventory.containsKey(compactionContext.getClusterID())) {
                    log.info("the connection is already there, not creating again");
                    Connection connection = ConnectionFactory.createConnection(this.getHbaseConfig(compactionContext));
                    connectionInventory.put(compactionContext.getClusterID(), connection);
                }
                else {
                    if(connectionInventory.get(compactionContext.getClusterID()).isClosed()) {
                        log.info("connection is closed, recreating it");
                        connectionInventory.remove(compactionContext.getClusterID());
                        Connection connection = ConnectionFactory.createConnection(this.getHbaseConfig(compactionContext));
                        connectionInventory.put(compactionContext.getClusterID(), connection);
                    }
                }
            log.debug("loaded connection for {}", compactionContext);
            } catch (IOException e) {
                log.error("Exception while bootstrapping connection with {} - error: {}", compactionContext.getClusterID(), e.getMessage());
            }
            catch (Exception e) {
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
