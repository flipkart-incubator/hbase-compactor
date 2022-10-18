package com.flipkart.yak.interfaces;

import com.flipkart.yak.commons.Report;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.core.CompactionRuntimeException;
import org.apache.hadoop.hbase.client.Connection;

public interface RegionSelectionPolicy extends Configurable {
    Report getReport(CompactionContext context, Connection connection) throws CompactionRuntimeException;
}
