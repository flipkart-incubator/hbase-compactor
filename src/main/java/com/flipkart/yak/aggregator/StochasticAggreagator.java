package com.flipkart.yak.aggregator;

import com.flipkart.yak.interfaces.PolicyAggregator;
import org.apache.hadoop.hbase.util.Pair;

import java.util.List;

public class StochasticAggreagator implements PolicyAggregator {

    @Override
    public void setFromConfig(List<Pair<String, String>> configs) {

    }
}
