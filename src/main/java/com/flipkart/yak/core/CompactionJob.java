package com.flipkart.yak.core;

import com.flipkart.yak.commons.ConnectionInventory;
import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfileConfig;
import com.flipkart.yak.config.CompactionSchedule;
import com.flipkart.yak.interfaces.PolicyAggregator;
import com.flipkart.yak.interfaces.RegionSelectionPolicy;
import com.flipkart.yak.interfaces.Submittable;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;

import java.util.List;
import java.util.Map;


@Slf4j
public abstract class CompactionJob implements Submittable {

    private List<RegionSelectionPolicy> policies;
    private PolicyAggregator aggregator;
    private Connection connection;
    private CompactionSchedule compactionSchedule;

    public abstract void compact(Report report);

    @Override
    public void init(CompactionContext context, Map<String, CompactionProfileConfig> profileInventory) {
       connection = ConnectionInventory.getInstance().get(context.getClusterID());
       compactionSchedule = context.getCompactionSchedule();

    }

    @Override
    public void run() {
        log.info("starting");

    }
}
