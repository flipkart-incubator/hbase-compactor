package com.flipkart.yak.commons;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Pair;

import java.util.HashMap;

public class Report extends HashMap<String, Pair<RegionInfo, RegionEligibilityStatus>> {
}
