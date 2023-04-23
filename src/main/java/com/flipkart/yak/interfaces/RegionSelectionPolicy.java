package com.flipkart.yak.interfaces;

import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.core.CompactionRuntimeException;

/**
 * Interface to implement algorithm for selecting regions eligible for compaction.
 * @param <Resource> ConfigResource required for the implementation to work.
 */
public interface RegionSelectionPolicy<Resource> extends Configurable {
    Resource init(CompactionContext compactionContext);
    Report getReport(CompactionContext context, Resource resource) throws CompactionRuntimeException;
    Report getReport(CompactionContext context, Resource resource, Report lastKnownReport) throws CompactionRuntimeException;
    void release(Resource resource);
}
