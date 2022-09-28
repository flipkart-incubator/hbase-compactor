package com.flipkart.yak.core;

import org.apache.hadoop.hbase.util.Pair;

import java.util.List;

public interface Configurable {
    void instantiateFrom(List<Pair<String, String>> configs);
}
