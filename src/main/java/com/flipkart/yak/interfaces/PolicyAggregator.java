package com.flipkart.yak.interfaces;

import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.core.CompactionRuntimeException;
import com.flipkart.yak.core.PolicyRunner;

import java.util.Set;

public interface PolicyAggregator extends Configurable {
    Report applyAndCollect(Set<RegionSelectionPolicy> allPolicies, PolicyRunner runner, CompactionContext compactionContext) throws CompactionRuntimeException;
}
