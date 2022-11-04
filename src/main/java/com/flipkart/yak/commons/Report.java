package com.flipkart.yak.commons;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Pair;

import java.util.HashMap;

@Getter
@AllArgsConstructor
public class Report extends HashMap<String, Pair<RegionInfo, RegionEligibilityStatus>> {
    @NonNull
    String preparedBy;
}
