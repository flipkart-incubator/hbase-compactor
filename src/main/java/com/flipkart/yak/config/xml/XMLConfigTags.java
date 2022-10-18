package com.flipkart.yak.config.xml;

import java.util.Arrays;

public enum XMLConfigTags {
    CONTEXTS_LIST_TAG("contexts"),
    CONTEXT_CLUSTER_ID("clusterID"),
    CONTEXT_TABLE_NAME("tableName"),
    CONTEXT_NAMESPACE("nameSpace"),
    CONTEXT_RSGROUP("rsGroup"),
    CONTEXT_PROFILE_ID("profileID"),
    CONTEXT_START_TIME("startTime"),
    CONTEXT_END_TIME("endTime"),
    PROFILE_LIST_TAG( "profile"),
    POLICY_LIST_TAG( "policies"),
    AGGREGATOR_TAG( "aggregator"),
    NAME_TAG("name"),
    VALUE_TAG("value"),
    CONFIG_LIST_TAGS("configurations"),
    CONFIG_TAGS("configuration");


    String name;
    XMLConfigTags(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return this.name;
    }

    public static XMLConfigTags getEnum(String enumName) {
        return Arrays.stream(XMLConfigTags.values()).filter(x -> x.name.equals(enumName)).findAny().orElse(null);
    }
}
