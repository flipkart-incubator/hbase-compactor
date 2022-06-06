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

## Run
```
java -jar <jar> <zk quorum> <zk node> <table name> <batch size> <sleep b/w batch in sec> <to_force> <max_num_regions_on_one_server>
```
