package com.flipkart.yak.interfaces;

import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.core.CompactionRuntimeException;

public interface RegionSelectionPolicy<Resource> extends Configurable {
    Resource init(CompactionContext compactionContext);
    Report getReport(CompactionContext context, Resource resource) throws CompactionRuntimeException;
    Report getReport(CompactionContext context, Resource resource, Report lastKnownReport) throws CompactionRuntimeException;
    void release(Resource resource);
}
