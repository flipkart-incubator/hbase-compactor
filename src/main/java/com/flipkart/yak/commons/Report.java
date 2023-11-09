package com.flipkart.yak.commons;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Pair;

import java.util.LinkedHashMap;

@Getter
@AllArgsConstructor
public class Report extends LinkedHashMap<String, Pair<RegionInfo, RegionEligibilityStatus>> {
    @NonNull
    String preparedBy;
}
