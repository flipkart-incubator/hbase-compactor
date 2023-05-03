package com.flipkart.yak.commons;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class ConnectionInventoryTest {

    @ParameterizedTest
    @MethodSource("provideStringsForTestConnectionInventoryZookeeperQuorum")
    void testConnectionInventoryZookeeperQuorum(String passedValue, String expectedValue)  {
        String quorum = ConnectionInventory.getZookeeperQuorum(passedValue);
        assert quorum != null;
        assert quorum.equals(expectedValue);
    }

    private static Stream<Arguments> provideStringsForTestConnectionInventoryZookeeperQuorum() {
        return Stream.of(
                Arguments.of("preprod-id-yak-perf1-ch-zk-1:2181,preprod-id-yak-perf1-ch-zk-2:2181,preprod-id-yak-perf1-ch-zk-3:2181:/preprod-id-yak-perf1",
                        "preprod-id-yak-perf1-ch-zk-1:2181,preprod-id-yak-perf1-ch-zk-2:2181,preprod-id-yak-perf1-ch-zk-3:2181"),
                Arguments.of("preprod-id-yak-perf1-ch-zk-1:2181:/hbase","preprod-id-yak-perf1-ch-zk-1:2181"),
                Arguments.of("preprod-id-yak-perf1-ch-zk-1:2181","preprod-id-yak-perf1-ch-zk-1:2181"),
                Arguments.of("localhost:2181","localhost:2181"),
                Arguments.of("prod-id-yak-order-zk-1:2181","prod-id-yak-order-zk-1:2181"),
                Arguments.of( "playground-core-ch-2-zk-0.playground-core-ch-2.yak-core-playground.svc.cluster.local:2181:/hbase", "playground-core-ch-2-zk-0.playground-core-ch-2.yak-core-playground.svc.cluster.local:2181")
        );
    }

    @Test
    void testConnectionInventoryInstance()  {
        ConnectionInventory connectionInventory1 = ConnectionInventory.getInstance();
        ConnectionInventory connectionInventory2 = ConnectionInventory.getInstance();
        ConnectionInventory connectionInventory3 = ConnectionInventory.getInstance();
        assert connectionInventory1 == connectionInventory3;
        assert connectionInventory2 == connectionInventory3;
    }

    @ParameterizedTest
    @MethodSource("provideStringsForTestConnectionInventoryKeyUniqueness")
    void testConnectionInventoryKeyUniqueness(String passedValue, String expectedValue)  {
        String quorum = ConnectionInventory.getUniqueStringFromID(passedValue);
        assert quorum != null;
        assert quorum.equals(expectedValue);
    }

    private static Stream<Arguments> provideStringsForTestConnectionInventoryKeyUniqueness() {
        return Stream.of(
                Arguments.of("preprod-id-yak-perf1-ch-zk-1:2181,preprod-id-yak-perf1-ch-zk-2:2181,preprod-id-yak-perf1-ch-zk-3:2181:/preprod-id-yak-perf1",
                        "preprod-id-yak-perf1-ch-zk-1:2181,preprod-id-yak-perf1-ch-zk-2:2181,preprod-id-yak-perf1-ch-zk-3:2181"),
                Arguments.of("preprod-id-yak-perf1-ch-zk-2:2181,preprod-id-yak-perf1-ch-zk-1:2181,preprod-id-yak-perf1-ch-zk-3:2181:/preprod-id-yak-perf1",
                        "preprod-id-yak-perf1-ch-zk-1:2181,preprod-id-yak-perf1-ch-zk-2:2181,preprod-id-yak-perf1-ch-zk-3:2181"),
                Arguments.of("preprod-id-yak-perf1-ch-zk-3:2181,preprod-id-yak-perf1-ch-zk-2:2181,preprod-id-yak-perf1-ch-zk-1:2181:/preprod-id-yak-perf1",
                        "preprod-id-yak-perf1-ch-zk-1:2181,preprod-id-yak-perf1-ch-zk-2:2181,preprod-id-yak-perf1-ch-zk-3:2181"),
                Arguments.of("preprod-id-yak-perf1-ch-zk-3:2181,preprod-id-yak-perf1-ch-zk-2:2181,preprod-id-yak-perf1-ch-zk-1:2181",
                        "preprod-id-yak-perf1-ch-zk-1:2181,preprod-id-yak-perf1-ch-zk-2:2181,preprod-id-yak-perf1-ch-zk-3:2181"),
                Arguments.of("preprod-id-yak-perf1-ch-zk-3:2181", "preprod-id-yak-perf1-ch-zk-3:2181"),
                Arguments.of("prod-id-yak-order-zk-1:2181,prod-id-yak-order-zk-2:2181", "prod-id-yak-order-zk-1:2181,prod-id-yak-order-zk-2:2181"),
                Arguments.of("prod-id-yak-order-zk-1:2181,prod-id-yak-order-zk-2:2181,prod-id-yak-order-zk-3:2181,prod-id-yak-order-zk-4:2181,prod-id-yak-order-zk-5:2181",
                        "prod-id-yak-order-zk-1:2181,prod-id-yak-order-zk-2:2181,prod-id-yak-order-zk-3:2181,prod-id-yak-order-zk-4:2181,prod-id-yak-order-zk-5:2181"),
                Arguments.of("prod-id-yak-order-zk-2:2181,prod-id-yak-order-zk-4:2181,prod-id-yak-order-zk-1:2181,prod-id-yak-order-zk-3:2181,prod-id-yak-order-zk-5:2181",
                        "prod-id-yak-order-zk-1:2181,prod-id-yak-order-zk-2:2181,prod-id-yak-order-zk-3:2181,prod-id-yak-order-zk-4:2181,prod-id-yak-order-zk-5:2181"),
                Arguments.of("prod-id-yak-order-zk-2:2181,prod-id-yak-order-zk-1:2181", "prod-id-yak-order-zk-1:2181,prod-id-yak-order-zk-2:2181")
        );
    }

    @Test
    void testConnectionInventoryKeyUniquenessWithPut()  {
        ConnectionInventory connectionInventory = ConnectionInventory.getInstance();
        connectionInventory.put("preprod-id-yak-perf1-ch-zk-1:2181,preprod-id-yak-perf1-ch-zk-2:2181,preprod-id-yak-perf1-ch-zk-3:2181:/preprod-id-yak-perf1", null);
        connectionInventory.put("preprod-id-yak-perf1-ch-zk-1:2181,preprod-id-yak-perf1-ch-zk-2:2181,preprod-id-yak-perf1-ch-zk-3:2181", null);
        connectionInventory.put("preprod-id-yak-perf1-ch-zk-1:2181,preprod-id-yak-perf1-ch-zk-3:2181,preprod-id-yak-perf1-ch-zk-2:2181:/preprod-id-yak-perf1", null);
        connectionInventory.put("preprod-id-yak-perf1-ch-zk-2:2181,preprod-id-yak-perf1-ch-zk-3:2181,preprod-id-yak-perf1-ch-zk-1:2181:/preprod-id-yak-perf1", null);
        connectionInventory.putIfAbsent("preprod-id-yak-perf1-ch-zk-2:2181,preprod-id-yak-perf1-ch-zk-1:2181,preprod-id-yak-perf1-ch-zk-3:2181:/preprod-id-yak-perf1", null);
        connectionInventory.putIfAbsent("preprod-id-yak-perf1-ch-zk-2:2181,preprod-id-yak-perf1-ch-zk-1:2181,preprod-id-yak-perf1-ch-zk-3:2181", null);
        assert connectionInventory.size() == 1;
    }
}
