package service;

import config.CompactorConfig;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.NoServerForRegionException;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static config.CompactorConfig.*;

public class CompactionService {
    private static Logger log = Logger.getLogger(CompactionService.class);

    private Admin admin;
    private ExecutorService executorService;
    private Connection connection;
    private CompactorConfig compactorConfig;
    private int batchSize;
    private int waitTime;

    public CompactionService(Connection connection, CompactorConfig config) throws IOException {
        this.batchSize = (int) config.getConfig(BATCH_SIZE_KEY);
        this.waitTime =  ((int)config.getConfig(WAIT_TIME_KEY))*1000;
        executorService = Executors.newFixedThreadPool(this.batchSize);
        this.admin = connection.getAdmin();
        this.compactorConfig = config;
        this.connection = connection;
    }


    public void start() throws IOException, InterruptedException {
        RegionFetcher regionFetcher = new RegionFetcher(connection, compactorConfig);

        log.info("starting major compaction");

        Map<String, List<String>> regionServers = regionFetcher.getNext();

        Map<String, String> previouslyTriggered = new HashMap<>();

        boolean done = false;
        int doneRegions=0;

        //TODO:  not checking for null for the first call: regionFetcher.getNext();
        while(!done || regionServers.size()!=0) {

            while (regionServers.size() < this.batchSize && !done) {
                Map<String, List<String>> nextRegionServers = regionFetcher.getNext();
                if (nextRegionServers != null && nextRegionServers.size() == 0) {
                    done = true;
                    break;
                }

                if(nextRegionServers != null) {
                    merge(regionServers, nextRegionServers);
                } else {
                    log.error("Regions fetch seem to have failed, will sleep and retry");
                    Thread.sleep(this.waitTime);
                }
            }

            int i = 0;

            List<Future> futures = new ArrayList<>();

            List<String> doneRegionServers = new ArrayList<>();

            for (Map.Entry<String, List<String>> entry : regionServers.entrySet()) {

                if (i > this.batchSize)
                    break;

                String regionName = entry.getValue().get(entry.getValue().size()-1);
                String regionServer = entry.getKey();

                if(previouslyTriggered.containsKey(regionServer)) {
                    try {
                        AdminProtos.GetRegionInfoResponse.CompactionState compactionState = admin.getCompactionStateForRegion(Bytes.toBytes(previouslyTriggered.get(regionServer)));
                        if (compactionState == AdminProtos.GetRegionInfoResponse.CompactionState.MAJOR) {
                            log.info("Major compaction still in progress on " + regionServer + " on region " + previouslyTriggered.get(regionServer));
                            i++;
                            continue;
                        }
                    } catch (IllegalArgumentException e) {
                        log.error("Region does not exist " + regionName + ", continuing compaction on " + regionServer);
                        e.printStackTrace();
                    } catch (NoServerForRegionException e) {
                        log.error("Region " + regionName + " is not served by any RS, continuing compaction on " + regionServer);
                        e.printStackTrace();
                    } catch (Exception e) {
                        log.error("Failed to check compaction status " + regionName + " on " + regionServer + ", continuing compaction on " + regionServer);
                        e.printStackTrace();
                    }
                }


                previouslyTriggered.put(regionServer, regionName);

                entry.getValue().remove(entry.getValue().size()-1);

                if(entry.getValue().size() == 0) {
                    doneRegionServers.add(entry.getKey());
                }

                log.info("Running compaction: " + regionName + " on " + regionServer);

                i++;
                doneRegions++;

                futures.add(executorService.submit(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        try {
                            admin.majorCompactRegion(Bytes.toBytes(regionName));
                        } catch (IOException e) {
                            throw new Exception(e);
                        }
                        return regionName + ":" + regionServer;
                    }
                }));
            }

            doneRegionServers.forEach(drs -> regionServers.remove(drs));

            for (Future future : futures) {
                try {
                    log.info("Triggered compaction: " + future.get());
                } catch (ExecutionException e) {
                    log.error(e);
                    e.printStackTrace();
                }
            }

            log.info("Sleeping for " + this.waitTime + " done:" + doneRegions);
            Thread.sleep(this.waitTime);
        }


        log.info("compaction complete");
    }

    private void merge(Map<String, List<String>> map1, Map<String, List<String>> map2) {
        for(Map.Entry<String, List<String>> entry : map2.entrySet()) {
            if(map1.containsKey(entry.getKey())) {
                map1.get(entry.getKey()).addAll(entry.getValue());
            } else {
                map1.put(entry.getKey(), entry.getValue());
            }
        }
    }
}
