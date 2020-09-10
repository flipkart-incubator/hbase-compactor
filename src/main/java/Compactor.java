import config.CompactorConfig;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;
import service.CompactionService;

import java.io.IOException;

public class Compactor {
    private static Logger log = Logger.getLogger(Compactor.class);

    public static void main(String args[])  {
        try {
            CompactorConfig compactorConfig = new CompactorConfig(args);
            Connection connection = ConnectionFactory.createConnection(compactorConfig.getHbaseConfig());
            new CompactionService(connection, compactorConfig).start();
        } catch (MissingArgumentException e) {
            log.error(e);
            System.exit(1);
        } catch (IOException | InterruptedException e) {
            log.error(e.getMessage(), e);
            System.exit(1);
        }
        System.exit(0);
    }
}
