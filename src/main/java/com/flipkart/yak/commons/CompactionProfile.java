package com.flipkart.yak.commons;

import com.flipkart.yak.interfaces.Configurable;
import com.flipkart.yak.interfaces.PolicyAggregator;
import com.flipkart.yak.interfaces.RegionSelectionPolicy;
import com.flipkart.yak.interfaces.Validable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.hbase.util.Pair;

import java.util.List;
import java.util.Set;


@Getter
@AllArgsConstructor
public class CompactionProfile implements Validable, Configurable {


    @NonNull
    final String ID;
    Set<RegionSelectionPolicy> policies;
    @NonNull final PolicyAggregator aggregator;


    @Override
    public void setFromConfig(List<Pair<String, String>> configs) {

    }

    @Override
    public void validate() throws ConfigurationException {

    }
}
