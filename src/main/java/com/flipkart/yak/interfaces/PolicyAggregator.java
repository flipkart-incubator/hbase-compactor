package com.flipkart.yak.interfaces;

import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.core.CompactionRuntimeException;
import com.flipkart.yak.core.PolicyRunner;

import java.util.Set;

/**
 * Interface to implement Policy {@link Report} aggregation algorithm.
 * One such possible implementation in {@link com.flipkart.yak.aggregator.ChainReportAggregator} which applies
 * {@link RegionSelectionPolicy} one after another in ordered fashion.
 * This is part of {@link com.flipkart.yak.commons.CompactionProfile} itself and only aggregator per profile can be applied.
 */
public interface PolicyAggregator extends Configurable {
    Report applyAndCollect(Set<RegionSelectionPolicy> allPolicies, PolicyRunner runner, CompactionContext compactionContext) throws CompactionRuntimeException;
}
