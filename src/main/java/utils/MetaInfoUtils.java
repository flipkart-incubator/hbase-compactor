package utils;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class MetaInfoUtils {

    private final static byte[] INFO_CF = Bytes.toBytes("info");
    private final static byte[] FN_CQ = Bytes.toBytes("fn");
    private final static String HBASE_NS = "hbase";
    private final static String META_TABLE = "meta";
    private Table metaTable;
    private static MetaInfoUtils metaInfoUtils;
    private Logger log = Logger.getLogger(MetaInfoUtils.class);

    private MetaInfoUtils(Connection connection) throws IOException {
        this.metaTable = connection.getTable(TableName.valueOf(HBASE_NS, META_TABLE));
    }

    public static MetaInfoUtils getInstance(Connection connection) throws IOException {
        if(metaInfoUtils == null) {
            metaInfoUtils = new MetaInfoUtils(connection);
        }
        return metaInfoUtils;
    }

    public void refreshFavoredNodesMapping(TableName tableName, Map<String, List<String>> regionFNHostnameMapping) throws IOException {
        Optional<PrefixFilter> filterOptional = Optional.of(new PrefixFilter(Bytes.toBytes(tableName + ",")));
        ResultScanner scanner = metaTable.getScanner(getScan(INFO_CF, FN_CQ, filterOptional));
        while (true) {
            Result[] results = scanner.next(10);
            if (results != null && results.length > 0) {
                for (int index = 0; index < results.length; index += 1) {
                    Result result = results[index];
                    List<String> fnHostsList = getFavoredNodesList(result.getValue(INFO_CF, FN_CQ));
                    String[] tokens = Bytes.toString(result.getRow()).split("\\.");
                    log.trace("Identified Region: " + tokens[tokens.length - 1] + " fns: " + fnHostsList);
                    if (fnHostsList.size() > 0) {
                        regionFNHostnameMapping.put(tokens[tokens.length - 1], fnHostsList);
                    } else {
                        regionFNHostnameMapping.putIfAbsent(tokens[tokens.length - 1], fnHostsList);
                    }
                }
            } else {
                break;
            }
        }
    }

    public List<String> getFavoredNodesList(byte[] favoredNodes) throws IOException {
        HBaseProtos.FavoredNodes f = HBaseProtos.FavoredNodes.parseFrom(favoredNodes);
        List<HBaseProtos.ServerName> protoNodes = f.getFavoredNodeList();
        ServerName[] servers = new ServerName[protoNodes.size()];
        int i = 0;
        for (HBaseProtos.ServerName node : protoNodes) {
            servers[i++] = ProtobufUtil.toServerName(node);
        }
        return Arrays.asList(servers).stream().map(server -> server.getHostname()).collect(Collectors.toList());
    }

    private Scan getScan(byte[] cf, byte[] column, Optional<PrefixFilter> filterOptional) {
        Scan scan = new Scan();
        scan.addColumn(cf, column);
        if (filterOptional.isPresent()) {
            scan.setFilter(filterOptional.get());
        }
        return scan;
    }
}
