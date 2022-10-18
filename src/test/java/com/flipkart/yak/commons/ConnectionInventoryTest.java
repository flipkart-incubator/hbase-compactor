package com.flipkart.yak.commons;

import org.junit.jupiter.api.Test;

public class ConnectionInventoryTest {

    @Test
    void testConnectionInventoryUniqueKey()  {
        String quorum = ConnectionInventory.getZookeeperQuorum("preprod-id-yak-perf1-ch-zk-1:2181,preprod-id-yak-perf1-ch-zk-2:2181,preprod-id-yak-perf1-ch-zk-3:2181:/preprod-id-yak-perf1");
        assert quorum.equals("preprod-id-yak-perf1-ch-zk-1:2181,preprod-id-yak-perf1-ch-zk-2:2181,preprod-id-yak-perf1-ch-zk-3:2181");
    }
}
