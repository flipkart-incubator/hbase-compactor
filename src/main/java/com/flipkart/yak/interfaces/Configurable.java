package com.flipkart.yak.interfaces;

import org.apache.hadoop.hbase.util.Pair;

import java.util.List;

/**
 * Marks a class can be configured from a list of Configuration on runtime. Especially useful for strategy classes like
 * {@link RegionSelectionPolicy} or {@link PolicyAggregator} to tune and customise the strategy on the go.
 */
public interface Configurable {

    /**
     *
     * @param configs
     */
    void setFromConfig(List<Pair<String, String>> configs);
}
