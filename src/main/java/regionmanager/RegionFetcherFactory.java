package regionmanager;

import config.CompactorConfig;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RegionFetcherFactory {
    private final Connection connection;
    private final CompactorConfig compactorConfig;
    private final Map<String, IRegionFetcher> iRegionFetcherMap;
    private final String DEFAULT_IREGIONFETCHER_STRING = "DEFAULT";
    private final String LOADAWARE_IREGIONFETCHER_STRING = "loadAware";
    private final Logger log = Logger.getLogger(this.getClass().getName());

    public RegionFetcherFactory(Connection connection, CompactorConfig compactorConfig) {
        this.connection = connection;
        this.compactorConfig = compactorConfig;
        this.iRegionFetcherMap = new HashMap<>();
    }

    public IRegionFetcher getRegionFetcher() throws IOException {
        try {
            this.iRegionFetcherMap.putIfAbsent(DEFAULT_IREGIONFETCHER_STRING, new SimpleRegionFetcher(this.connection, this.compactorConfig));
        } catch (IOException e) {
            log.error("could not bootstrap" + DEFAULT_IREGIONFETCHER_STRING +" RegionFetcher " + e.getMessage(), e);
            throw new IOException(e);
        }
        return this.iRegionFetcherMap.get(DEFAULT_IREGIONFETCHER_STRING);
    }

    public IRegionFetcher getRegionFetcher(String strategy) throws IOException {
        try {
            if (strategy.equals(LOADAWARE_IREGIONFETCHER_STRING)){
                this.iRegionFetcherMap.putIfAbsent(LOADAWARE_IREGIONFETCHER_STRING, new LoadAwareRegionFetcher(this.connection, this.compactorConfig));
            }
            else {
                return this.getRegionFetcher();
            }

        } catch (IOException e) {
            log.error("could not bootstrap "+ LOADAWARE_IREGIONFETCHER_STRING +" RegionFetcher " + e.getMessage(), e);
            throw new IOException(e);
        }
        return this.iRegionFetcherMap.get(DEFAULT_IREGIONFETCHER_STRING);
    }

}
