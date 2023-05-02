## hbase-compactor

Hbase Major compaction is a resource intensive operation. One would like to control the compaction with lot more flexibility than what hbase provides. One such attempt is this tool `hbase-compactor`. Some of the important factors which can be controlled are

* How many parallel region compactions
* If favored nodes are applicable, how many parallel regions with non overlapping favored nodes.
* Delay between 2 region compactions
* Can be configured using a cron job. 

## Build
```
mvn clean package 
```

## Sample Config File
```
<configuration>
    <contexts>
        <context>
            <clusterID>zookeeper:port:hbase_root_path</clusterID>
            <tableName>table_name</tableName>
            <namespace>namespace</namespace>
            <startTime>16.5</startTime>
            <endTime>17.00</endTime>
            <profileID>testID</profileID>
        </context>
    </contexts>
    <profiles>
        <profile>
            <profileID>testID</profileID>
            <policies>
                <policy>
                    <name>com.flipkart.yak.policies.NaiveRegionSelectionPolicy</name>
                    <configurations>
                        <configuration>
                            <name>compactor.policy.max.parallel.compaction.per.server</name>
                            <value>2</value>
                        </configuration>
                    </configurations>
                </policy>
                <policy>
                    <name>com.flipkart.yak.policies.TimestampAwareSelectionPolicy</name>
                </policy>
            </policies>
            <aggregator>
                <name>com.flipkart.yak.aggregator.ChainReportAggregator</name>
                <configurations>
                    <configuration>
                        <name>aggregator.chain.policy.order</name>
                        <value>com.flipkart.yak.policies.TimestampAwareSelectionPolicy,com.flipkart.yak.policies.NaiveRegionSelectionPolicy</value>
                    </configuration>
                </configurations>
            </aggregator>
        </profile>
    </profiles>
</configuration>
```
