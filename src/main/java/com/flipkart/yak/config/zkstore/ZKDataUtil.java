package com.flipkart.yak.config.zkstore;

import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfileConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.util.MurmurHash3;

import java.util.UUID;

@Slf4j
public class ZKDataUtil {

    public static final String BASE_PATH = "/hbase-compactor";
    public static final String CONTEXT_PATH = "/contexts";
    public static final String PROFILE_PATH = "/profiles";
    public static final String CONTEXT_DELIMITER = "-";

    public static final int DEFAULT_SESSION_TIMEOUT = 10000;
    public static byte[] getSerializedContext(CompactionContext compactionContext) {
        return null;
    }
    public static byte[] getSerializedProfile(CompactionProfileConfig compactionProfileConfig) {
        return null;
    }

    public static CompactionContext getContextFromSerializedConfig(byte[] data) {
        return null;
    }

    public static CompactionProfileConfig getProfileFromSerializedConfig(byte[] data) {
        return null;
    }

    public static String getProfilePath (String profileID) {
        String path = BASE_PATH + PROFILE_PATH + "/"+ profileID;
        log.info("creating path for compaction profile: {}", path);
        return path;
    }

    public static String getContextPath (CompactionContext compactionContext) {
        String key = compactionContext.getClusterID() + CONTEXT_DELIMITER +compactionContext.getRsGroup() +
                CONTEXT_DELIMITER + compactionContext.getNameSpace() + CONTEXT_DELIMITER + compactionContext.getTableName();
        UUID hash = UUID.fromString(key);
        String path = BASE_PATH + PROFILE_PATH + "/"+ hash;
        log.info("creating path for compaction context: {}", path);
        return path;
    }
}
