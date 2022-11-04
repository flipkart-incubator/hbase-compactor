package com.flipkart.yak.interfaces;

import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.core.CompactionRuntimeException;

import java.util.Set;

public interface PolicyAggregator extends Configurable {
    Report applyAndCollect(Set<RegionSelectionPolicy> allPolicies, CompactionContext compactionContext) throws CompactionRuntimeException;
}
