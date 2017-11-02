# hbase-compactor
Normally huge clusters which are very particular about peformance have major compactions turned off. Currenlty, to turn on the major compaction config need to be changed, which require whole cluster restart. Moreover, major compaction once triggered cannot be stopped without cluster restart.

This utility trigger major compaction in hbase in tightly controlled manner where you can start/stop compaction without cluster restart. It ensures that only 1 major compaction is running at a time on a single region server and try to parallelize it with concurrency of <batch size> (default 1) on whole hbase cluster.

Actual parallelism =  min(number of region servers, batch size)

# Build
```
mvn clean package
```

# Run
```
java -jar <jar> <zk quorum> <zk node> <table name> [batch size default=1] [sleep b/w batch in sec default=600]
```

# Tested versions

* 0.98
* 1.1.2


