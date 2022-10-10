package com.flipkart.yak.policies;

import com.flipkart.yak.interfaces.RegionSelectionPolicy;
import org.apache.hadoop.hbase.util.Pair;

import java.util.List;

public class NaiveRegionSelectionPolicy implements RegionSelectionPolicy {
    @Override
    public void setFromConfig(List<Pair<String, String>> configs) {

    }
}
