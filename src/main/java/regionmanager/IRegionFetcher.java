package regionmanager;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface IRegionFetcher {
    public List<String> getNextBatchOfEncodedRegions(Set<String> inProgressRegions) throws IOException;
}
