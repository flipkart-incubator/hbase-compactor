package com.flipkart.yak.interfaces;


import com.flipkart.yak.config.CompactionContext;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.hbase.security.User;


public interface Submittable extends Runnable {
    void init(CompactionContext compactionContext, User user) throws ConfigurationException;
}
