package com.flipkart.yak.commons.hadoop;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;

@Getter
@AllArgsConstructor
public class HDFSConnection {
    Connection connection;
    ClientProtocol hdfsConnection;
}
