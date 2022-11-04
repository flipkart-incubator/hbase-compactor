package com.flipkart.yak.interfaces;

import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.core.CompactionRuntimeException;
import org.apache.commons.configuration.ConfigurationException;

public interface CompactionExecutable {
    void initResources(CompactionContext compactionContext) throws ConfigurationException;
    void doCompact(Report report) throws CompactionRuntimeException;
    void releaseResources();
}
