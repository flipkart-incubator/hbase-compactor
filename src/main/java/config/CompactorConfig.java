package config;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Properties;

public class CompactorConfig {
  public static final int DEFAULT_WAIT = 60;                           // seconds
  public static final int RECOMPACT_REGION_HOURS = 6;                   // no recompaction within 6 hours

  public static final String TABLE_NAME_KEY = "table_name";           // table name to compact
  public static final String BATCH_SIZE_KEY = "batch_size";           // number of regions to compact concurrently
  public static final String ZK_QUORUM_KEY = "zookeeper_quorum";      // zk quorum
  public static final String ZK_NODE_KEY = "znode";                   // zk node
  public static final String WAIT_TIME_KEY = "wait_time";             // sleep between each batch
  public static final String FORCE_COMPACTION = "force_compaction";   // force compaction if within recompaction gap
  public static final String REGION_SELECTION_STRATEGY = "region_fetcher";

  private Properties properties;

  public CompactorConfig(String[] args) throws MissingArgumentException {
    if (args.length < 5) {
      throw new MissingArgumentException(
          "Usage: <zk_quorum> <zk_node> <table_name> [num_compactions] [delay] [force]\n\n"
              + "zk_quorum: Zookeeper quorum. Example: preprod-id-yak-testbed-ch-zk-1,preprod-id-yak-testbed-ch-zk-2\n\n"
              + "zk_node: Zookeeper znode. Example: /hbase\n\n" + "table_name: Table name to compact\n\n"
              + "num_compactions: number of region compactions in parallel.\n\n"
              + "delay: Delay between 2 compaction triggers. Default: 600 seconds\n\n"
              + "region_fetcher: strategy to be used to select batch of regions"
              + "force: region compacted in last 3 hours will be ignored unless this flag is passed. Optional: true\n");
    }
    properties = new Properties();
    properties.put(ZK_QUORUM_KEY, args[0]);
    properties.put(ZK_NODE_KEY, args[1]);
    properties.put(TABLE_NAME_KEY, args[2]);
    properties.put(BATCH_SIZE_KEY, Integer.valueOf(args[3]));
    properties.put(WAIT_TIME_KEY, Integer.valueOf(args[4]) < 300 ? DEFAULT_WAIT : Integer.valueOf(args[4]));
    properties.put(REGION_SELECTION_STRATEGY, args[5]);
    properties.put(FORCE_COMPACTION, (args.length > 5 && args[6].equals("force")) ? true : false);
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
