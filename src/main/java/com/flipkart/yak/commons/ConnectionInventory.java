package com.flipkart.yak.commons;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class ConnectionInventory extends HashMap<String, Connection> {

    private static ConnectionInventory connectionInventory = new ConnectionInventory();
    private static final String ZOOKEEPER_HOST_SEPARATOR = ",";
    private static final String ZOOKEEPER_PARENT_SEPARATOR = "/";
    private static final String DEFAULT_PARENT = "hbase";
    private static final String ZOOKEEPER_PORT_SEPARATOR = ":";
    private static final String HOSTNAME_LIST_REGEX = "([\\w\\-]+:[\\d]{4},){1,5}";
    private static final Pattern clusterIDRegexPattern = Pattern.compile(HOSTNAME_LIST_REGEX);

    private ConnectionInventory(){};

    @Override
    public Connection put(String key, Connection value) {
        String target = getUniqueStringFromID(key);
        log.debug("adding Connection with key {}", target);
        return super.put(target, value);
    }

    @Override
    public Connection get(Object key) {
        String target = getUniqueStringFromID((String)key);
        log.debug("getting Connection with key {}", target);
        return super.get(target);
    }

    @Override
    public Connection putIfAbsent(String key, Connection value) {
        String target = getUniqueStringFromID((String)key);
        return super.putIfAbsent(target, value);
    }

    public static String getUniqueStringFromID(String clusterID) {
        String quorum = getZookeeperQuorum(clusterID);
        return quorum;
    }

    public static String getParentFromID(String clusterID) {
        Pair<String,String> hostandpath = separatePathAndHost(clusterID);
        if (hostandpath !=null) {
            return hostandpath.getSecond();
        }
        return null;
    }

    public static String getZookeeperQuorum(String clusterID) {
        List<String> list = prepareSortedListOfZookeepers(clusterID);
        if (list !=null) {
            return String.join(ZOOKEEPER_HOST_SEPARATOR, list);
        }
        return null;
    }

    private static List<String> prepareSortedListOfZookeepers(String clusterID) {
        Pair<String,String> hostandpath = separatePathAndHost(clusterID);
        if (hostandpath != null) {
            String[] listOfZookeeper = hostandpath.getFirst().split(ZOOKEEPER_HOST_SEPARATOR);
            List<String> listedFormat = Lists.newArrayList(listOfZookeeper);
            Collections.sort(listedFormat);
            return listedFormat;
        }
        return null;
    }

    private static Pair<String,String> separatePathAndHost(String clusterID) {
        String[] separatedPath = clusterID.split(ZOOKEEPER_PARENT_SEPARATOR);
        String path = DEFAULT_PARENT;
        if (separatedPath.length > 1) {
            path = separatedPath[1];
        }
        String zkString = separatedPath[0];
        if (separatedPath[0].endsWith(":")) {
            zkString = separatedPath[0].substring(0, separatedPath[0].length()-1);
        }
        Matcher matcher = clusterIDRegexPattern.matcher(zkString+ZOOKEEPER_HOST_SEPARATOR);
        if(matcher.matches()) {
            return new Pair<>(zkString, ZOOKEEPER_PARENT_SEPARATOR + path);
        }
        return null;
    }


    public static ConnectionInventory getInstance() {
        return connectionInventory;
    }
}
