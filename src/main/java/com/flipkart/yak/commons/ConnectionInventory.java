package com.flipkart.yak.commons;

import org.apache.hadoop.hbase.client.Connection;

import java.util.HashMap;

public class ConnectionInventory extends HashMap<String, Connection> {

    private static ConnectionInventory connectionInventory = new ConnectionInventory();

    private ConnectionInventory(){};

    @Override
    public Connection put(String key, Connection value) {
        String target = this.prepareUniqueStringFromID(key);
        return this.put(target, value);
    }

    @Override
    public Connection get(Object key) {
        String target = this.prepareUniqueStringFromID((String)key);
        return this.get(target);
    }

    private String prepareUniqueStringFromID(String clusterID) {
        return null;
    }

    public static ConnectionInventory getInstance() {
        return connectionInventory;
    }
}
