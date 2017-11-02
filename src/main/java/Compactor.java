import config.CompactorConfig;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.log4j.Logger;
import service.CompactionService;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class Compactor {
    private static Logger log = Logger.getLogger(Compactor.class);

    public static void main(String args[])  {
        try {
            CompactorConfig compactorConfig = new CompactorConfig(args);
            Connection connection = ConnectionFactory.createConnection(compactorConfig.getHbaseConfig());
            new CompactionService(connection, compactorConfig).start();
        } catch (IOException | InterruptedException  | MissingArgumentException e) {
            log.error(e);
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}
