package regionmanager;

import config.CompactorConfig;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class LoadAwareRegionFetcher implements IRegionFetcher {
    public LoadAwareRegionFetcher(Connection connection, CompactorConfig compactorConfig) {
    }

    @Override
    public List<String> getNextBatchOfEncodedRegions(Set<String> inProgressRegions) throws IOException {
        return null;
    }
}
