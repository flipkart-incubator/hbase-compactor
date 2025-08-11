package com.flipkart.yak.config.zkstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfileConfig;
import com.google.common.hash.Hashing;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.utils.ZKPaths;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Slf4j
public class ZKDataUtil {

    public static final String BASE_PATH = "hbase-compactor";
    private static final String CONTEXT_PATH = "/contexts";
    private static final String PROFILE_PATH = "/profiles";
    public static final String CONTEXT_DELIMITER = "-";

    private static final ObjectMapper serDeserManager = new ObjectMapper();

    // TODO: should be made part of config
    public static final int DEFAULT_SESSION_TIMEOUT = 90000;
    public static final int DEFAULT_CONNECTION_TIMEOUT = 90000;
    public static byte[] getSerializedContext(CompactionContext compactionContext) {
        try {
            return  serDeserManager.writeValueAsBytes(compactionContext);
        } catch (JsonProcessingException e) {
            log.error("could not serialize into bytes: {}", e.getMessage());
        }
        return null;
    }
    public static byte[] getSerializedProfile(CompactionProfileConfig compactionProfileConfig) {
        try {
            return serDeserManager.writeValueAsBytes(compactionProfileConfig);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static CompactionContext getContextFromSerializedConfig(byte[] data) throws IOException {
        return serDeserManager.readValue(data, CompactionContext.class);
    }

    public static CompactionProfileConfig getProfileFromSerializedConfig(byte[] data) throws IOException {
        return serDeserManager.readValue(data, CompactionProfileConfig.class);
    }

    public static String getProfilePath (String profileID) {
        String path = ZKPaths.makePath(getProfileBasePath(), profileID);
        log.info("creating path for compaction profile: {}", path);
        return path;
    }

    public static String getContextPath (CompactionContext compactionContext) {
        String key = compactionContext.getClusterID() + CONTEXT_DELIMITER +compactionContext.getRsGroup() +
                CONTEXT_DELIMITER + compactionContext.getNameSpace() + CONTEXT_DELIMITER +
                (compactionContext.getTableNames() != null ? compactionContext.getTableNames() : "all-tables");
        String hashCode = Hashing.murmur3_32().hashString(key, StandardCharsets.UTF_8).toString();
        String path = ZKPaths.makePath(getContextBasePath(), hashCode);
        log.info("creating path for compaction context: {}", path);
        return path;
    }

    public static String getContextBasePath() {
        return ZKPaths.PATH_SEPARATOR+BASE_PATH+CONTEXT_PATH;
    }

    public static String getProfileBasePath() {
        return ZKPaths.PATH_SEPARATOR+BASE_PATH+PROFILE_PATH;
    }

    public static String getBasePath() {return  ZKPaths.PATH_SEPARATOR+BASE_PATH;}
}
