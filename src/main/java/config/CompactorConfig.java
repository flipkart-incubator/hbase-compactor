package config;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;


public class CompactorConfig {
    public static final int DEFAULT_BATCH_SIZE=1;
    public static final int DEFAULT_WAIT=600;                           // seconds

    public static final String TABLE_NAME_KEY = "table_name";           // table name to compact
    public static final String BATCH_SIZE_KEY = "batch_size";           // number of regions to compact concurrently
    public static final String ZK_QUORUM_KEY = "zookeeper_quorum";      // zk quorum
    public static final String ZK_NODE_KEY = "znode";                   // zk node
    public static final String WAIT_TIME_KEY = "wait_time";             // sleep between each batch

    private Properties properties;

    public CompactorConfig(String[] args) throws MissingArgumentException {
        if(args.length < 4) {
            throw new MissingArgumentException("Min args expected: <zk quorum> <zk node> <table name> <col family>");
        }
        properties = new Properties();
        properties.put(ZK_QUORUM_KEY, args[0]);
        properties.put(ZK_NODE_KEY, args[1]);
        properties.put(TABLE_NAME_KEY, args[2]);

        if(args.length > 3)
            properties.put(BATCH_SIZE_KEY, Integer.valueOf(args[3]));
        else
            properties.put(BATCH_SIZE_KEY, DEFAULT_BATCH_SIZE);

        if(args.length > 4)
            properties.put(WAIT_TIME_KEY, Integer.valueOf(args[4]));
        else
            properties.put(WAIT_TIME_KEY, DEFAULT_WAIT);
    }

    public Object getConfig(String key) {
        return properties.get(key);
    }

    public Configuration getHbaseConfig() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.clear();
        configuration.set("hbase.zookeeper.quorum", (String) getConfig(ZK_QUORUM_KEY));
        configuration.set("zookeeper.znode.parent", (String) getConfig(ZK_NODE_KEY));
        return configuration;
    }
}
