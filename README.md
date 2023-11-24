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

## Hbase Compactor API Details

### Overview

It provides details about APIs that hbase compactor currently supports and their usage

**Base URL**

{{COMPACTOR_URL}}/api/manage

**Endpoints**

1. Fetch all compaction contexts

*Endpoint* : /contexts 
*Method* : GET
*Parameters:*

Example Response:

    {
        "clusterID": "preprod-hbase-cluster-endpoint:2181",
        "compactionSchedule": {
            "startHourOfTheDay": 14.0,
            "endHourOfTheDay": 16.0,
            "prompt": false,
            "promptSchedule": null
        },
        "tableName": "testTable",
        "nameSpace": "test",
        "rsGroup": "default",
        "compactionProfileID": "default"
    }
    
2. Fetch all compaction profiles

*Endpoint* : /profiles 
*Method* : GET

Example Response:

    {
        "policies": [
            {
                "first": "com.org.hbase.policies.TimestampAwareSelectionPolicy",
                "second": []
            },
            {
                "first": "com.org.hbase.policies.NaiveRegionSelectionPolicy",
                "second": [
                    {
                        "first": "compactor.policy.max.parallel.compaction.per.server",
                        "second": "4"
                    },
                    {
                        "first": "compactor.policy.max.parallel.compaction.per.table",
                        "second": "5"
                    }
                ]
            }
        ],
        "aggregator": {
            "first": "com.org.hbase.aggregator.ChainReportAggregator",
            "second": [
                {
                    "first": "aggregator.chain.policy.order",
                    "second": "com.org.hbase.policies.TimestampAwareSelectionPolicy,com.flipkart.yak.policies.NaiveRegionSelectionPolicy"
                }
            ]
        },
        "id": "default"
    }

3. Add Compaction Context

*Endpoint* : /context
*Method* : POST
Example Payload Request:
    {
       "clusterID": "preprod-hbase-cluster-endpoint:2181",
       "compactionSchedule": {
       "startHourOfTheDay": 1.0,
       "endHourOfTheDay": 2.0
        },
       "tableName": "testTable",
       "nameSpace": "test",
       "rsGroup": "default",
       "compactionProfileID": "default"
    }

4. Add Compaction Profile

*Endpoint* : /profile
*Method* : POST
Example Payload Request:

    {
       "policies": [
            {
                "first": "com.org.hbase.policies.TimestampAwareSelectionPolicy",
                "second": []
            },
            {
                "first": "com.org.hbase.policies.NaiveRegionSelectionPolicy",
                "second": [
                	{
                		"first":"compactor.policy.max.parallel.compaction.per.server",
                		"second":"2"
                	}
                ]
            }
        ],
        "aggregator": {
            "first": "com.org.hbase.aggregator.ChainReportAggregator",
            "second": [
                {
                    "first": "aggregator.chain.policy.order",
                    "second": "com.org.hbase.policies.TimestampAwareSelectionPolicy,com.flipkart.yak.policies.NaiveRegionSelectionPolicy"
                }
            ]
        },
        "id": "default"
    }

5. Delete Compaction Context

*Endpoint* : /context
*Method* : DELETE
Example Payload Request:
    {
       "clusterID": "preprod-hbase-cluster-endpoint:2181",
       "compactionSchedule": {
       "startHourOfTheDay": 1.0,
       "endHourOfTheDay": 2.0
        },
       "tableName": "testTable",
       "nameSpace": "test",
       "rsGroup": "default",
       "compactionProfileID": "default"
    }
    
6. Trigger Immediate(prompt) Compaction on Demand

*Endpoint* : /trigger
*Method* : POST
Example Payload Request:
    {
       "clusterID": "preprod-hbase-cluster-endpoint:2181:2181",
       "duration": 2.0,
       "tableName": "testTable",
       "nameSpace": "test",
       "rsGroup": "default",
       "compactionProfileID": "default"
    }    

6. Delete all stale compaction Contexts
It deletes all stale compaction contexts that were promptly triggered and have been completed and their lifsespan has been ended.
*Endpoint* : /deleteAllStaleContexts
*Method* : DELETE
       




