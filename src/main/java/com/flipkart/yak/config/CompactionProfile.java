package com.flipkart.yak.config;

import com.flipkart.yak.interfaces.Configurable;
import com.flipkart.yak.interfaces.PolicyAggregator;
import com.flipkart.yak.interfaces.RegionSelectionPolicy;
import com.flipkart.yak.interfaces.Validable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.hbase.util.Pair;

import java.util.List;
import java.util.Objects;
import java.util.Set;

@Getter @Setter
@AllArgsConstructor
public class CompactionProfile implements Validable, Configurable {
    @NonNull final String ID;
    Set<RegionSelectionPolicy> policies;
    @NonNull final PolicyAggregator aggregator;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompactionProfile)) return false;
        CompactionProfile that = (CompactionProfile) o;
        return getID().equals(that.getID()) &&
                getAggregator().equals(that.getAggregator());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getID());
    }

    @Override
    public void validate() throws ConfigurationException {
        if (policies.size() == 0) {
            throw new ConfigurationException("no policies specified");
        }
    }

    @Override
    public void setFromConfig(List<Pair<String, String>> configs) {

    }
}
