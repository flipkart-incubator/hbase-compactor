package com.flipkart.yak.config;

import com.flipkart.yak.core.PolicyAggregator;
import com.flipkart.yak.core.RegionSelectionPolicy;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@Getter @Setter
@AllArgsConstructor
public class CompactionProfile {
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
}
