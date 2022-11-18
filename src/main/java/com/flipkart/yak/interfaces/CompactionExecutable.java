package com.flipkart.yak.interfaces;

import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.core.CompactionRuntimeException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface to actual compaction executor.  {@link com.flipkart.yak.core.CompactionManager} should
 * rely on this interface in order to invoke the compaction once the {@link Report}s are ready.
 * This is to isolate execution of compaction from trigger. Moving ahead, if Scheduler moves away
 * from Thread.sleep() approach, and takes event Driven approach, the underlying compaction calling mechanism will
 * remain same.
 */
@InterfaceAudience.Private
public interface CompactionExecutable {
    /**
     * This method is responsible for bootstraping all required connection to the cluster.
     * Register plugins if need, as in MetricRegistry etc.
     * @param compactionContext Compaction Task given to thread.
     * @throws ConfigurationException If bootstrap fails, due to wrong config.
     */
    void initResources(CompactionContext compactionContext) throws ConfigurationException;

    /***
     * Defines how {@link org.apache.hadoop.hbase.client.Admin} will be used to call <b>majorCompactRegion</b>
     * Multiple implementation like, Sequential, Parallel etc possible.
     * @param report on which the compaction will be performed.
     * @throws CompactionRuntimeException when fails to perform.
     */
    void doCompact(Report report) throws CompactionRuntimeException;

    /**
     * Tears down all connections, also release resources if holding any.
     */
    void releaseResources();
}
