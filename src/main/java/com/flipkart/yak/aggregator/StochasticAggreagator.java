package com.flipkart.yak.aggregator;

import com.flipkart.yak.core.Configurable;
import com.flipkart.yak.core.PolicyAggregator;
import org.apache.hadoop.hbase.util.Pair;

import java.util.List;

public class StochasticAggreagator implements Configurable, PolicyAggregator {
    @Override
    public void instantiateFrom(List<Pair<String, String>> configs) {

    }
}
