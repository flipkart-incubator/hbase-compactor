package com.flipkart.yak.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.yak.interfaces.Validable;
import com.google.common.collect.Sets;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.configuration.ConfigurationException;

import java.util.Objects;
import java.util.Set;

@Data
@SuperBuilder
@AllArgsConstructor
@Jacksonized
public class CompactionProfileConfig implements Validable {

    @NonNull final String ID;


    Set<SerializedConfigurable> policies;


    @NonNull final SerializedConfigurable aggregator;


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompactionProfileConfig)) return false;
        CompactionProfileConfig that = (CompactionProfileConfig) o;
        return getID().equals(that.getID()) &&
                Sets.difference(policies, that.getPolicies()).size() == 0 &&
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
    public String toString() {
        return "{" +
                "ID='" + ID + '\'' +
                ", policies=" + policies +
                ", aggregator=" + aggregator +
                '}';
    }
}
