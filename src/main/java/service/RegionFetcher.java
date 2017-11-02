package service;

import config.CompactorConfig;
import org.apache.commons.math3.geometry.partitioning.Region;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static config.CompactorConfig.BATCH_SIZE_KEY;
import static config.CompactorConfig.TABLE_NAME_KEY;

public class RegionFetcher {

    private static Logger log = Logger.getLogger(RegionFetcher.class);

    private Table metaTable;
    private int batchSize;
    private byte[] lastRow;
    private String tableName;

    public RegionFetcher(Connection connection, CompactorConfig config) throws IOException {
        this.metaTable = connection.getTable(TableName.valueOf("hbase","meta"));
        this.batchSize = (int) config.getConfig(BATCH_SIZE_KEY);
        this.tableName = (String) config.getConfig(TABLE_NAME_KEY);
    }

    public HashMap<String, List<String>> getNext() {

        HashMap<String, List<String>> regionServers = new HashMap<>();
        ResultScanner scanner = null;

        try {
            scanner = metaTable.getScanner(getScan());
        } catch (IOException e) {
            log.error("Failed to create scanner: " + e);
            e.printStackTrace();
            return null;
        }

        for(int i=0;i<batchSize;i++) {
            Result result = null;
            try {
                result = scanner.next();
            } catch (IOException e) {
                log.error(e);
                e.printStackTrace();
                scanner.close();
                return null;
            }
            if(result != null) {
                List<Cell> serverCells= result.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("server"));

                for(Cell cell : serverCells) {
                    String serverName = Bytes.toString(cell.getValue());
                    String[] tok = Bytes.toString(cell.getRow()).split("\\.");
                    String regionName = tok[tok.length-1];


                    if(regionServers.containsKey(serverName)) {
                        regionServers.get(serverName).add(regionName);
                    } else {
                        regionServers.put(serverName, new ArrayList<>());
                        regionServers.get(serverName).add(regionName);
                    }
                    lastRow = getNextStartRow(cell.getRow());
                }
            }
        }
        scanner.close();
        return regionServers;
    }

    private byte[] getNextStartRow(byte[] row) {
        byte[] startRow = new byte[row.length + 1];

        System.arraycopy(row, 0, startRow, 0, row.length);
        startRow[row.length] = (byte) 0;
        return startRow;
    }

    public Scan getScan() {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("server"));
        scan.setFilter(new PrefixFilter(Bytes.toBytes(tableName + ",")));

        if(lastRow != null)
            scan.setStartRow(lastRow);
        return scan;
    }
}
