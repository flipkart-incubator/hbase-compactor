package com.flipkart.yak.config;

import org.apache.hadoop.hbase.util.Pair;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SerializedConfigurable extends Pair<String , List<Pair<String, String>>> {

    public SerializedConfigurable(String a, List<Pair<String, String>> b) {
        super(a, b);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof SerializedConfigurable)) {
            return false;
        }
        SerializedConfigurable that = (SerializedConfigurable)other;
        boolean keyEqual = this.getFirst().equals(that.getFirst());
        if (!keyEqual) {
            return false;
        }
        List<String> thisSorted = this.getSecond().stream().map(Pair::toString).collect(Collectors.toList());
        List<String> thatSorted = that.getSecond().stream().map(Pair::toString).collect(Collectors.toList());
        Collections.sort(thisSorted);
        Collections.sort(thatSorted);
        if (thisSorted.size() != thatSorted.size()) {
            return false;
        }
        for (int i=0; i< thisSorted.size(); i++) {
            if (!thisSorted.get(i).equals(thatSorted.get(i))) {
                return false;
            }
        }
        return true;
    }
}
