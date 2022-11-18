package com.flipkart.yak.commons.hadoop;

import lombok.NoArgsConstructor;

import java.util.HashMap;

@NoArgsConstructor
public class HDFSDefaults extends HashMap<String, String> {

    public static String KEY_DFS_REPLICATION="dfs.replication";
    public static String KEY_DFS_REPLICATION_MAX="dfs.replication.max";
    public static String KEY_DFS_PERMISSION="dfs.permissions";
    public static String KEY_DFS_PERMISSIONS_SUPERUSERGROUP="dfs.permissions.superusergroup";
    public static String KEY_DFS_PERMISSIONS_LOCAL_PATH="dfs.block.local-path-access.user";
    public static String KEY_DFS_NAMESERVICE="dfs.nameservices";
    public static String KEY_DFS_NAMESERVICE_DEFAULT_STORE="dfs.ha.namenodes.yak-store";
    public static String KEY_DFS_NAMENODE_RPC_NN_1="dfs.namenode.rpc-address.yak-store.nn1";
    public static String KEY_DFS_NAMENODE_RPC_NN_2="dfs.namenode.rpc-address.yak-store.nn2";
    public static String KEY_DFS_NAMENODE_HTTP_NN_1="dfs.namenode.http-address.yak-store.nn1";
    public static String KEY_DFS_NAMENODE_HTTP_NN_2="dfs.namenode.http-address.yak-store.nn2";
    public static String KEY_FS_DEFAULT="fs.default.name";


    public void loadDefaults() {
        this.put(KEY_DFS_REPLICATION, "3");
        this.put(KEY_DFS_REPLICATION_MAX, "3");
        this.put(KEY_DFS_PERMISSION, "true");
        this.put(KEY_DFS_PERMISSIONS_SUPERUSERGROUP, "yak");
        this.put(KEY_DFS_PERMISSIONS_LOCAL_PATH, "yak");
        this.put(KEY_DFS_NAMESERVICE, "yak-store");
        this.put(KEY_DFS_NAMESERVICE_DEFAULT_STORE, "nn1,nn2");
        this.put(KEY_DFS_NAMENODE_RPC_NN_1, "localhost:8020");
        this.put(KEY_DFS_NAMENODE_RPC_NN_2, "localhost:8020");
        this.put(KEY_DFS_NAMENODE_HTTP_NN_1, "localhost:50070");
        this.put(KEY_DFS_NAMENODE_HTTP_NN_2, "localhost:50070");
        this.put(KEY_FS_DEFAULT, "hdfs://yak-store");
    }

}
