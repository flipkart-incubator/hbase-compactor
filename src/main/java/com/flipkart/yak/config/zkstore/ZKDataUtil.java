package com.flipkart.yak.config.zkstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfileConfig;
import com.google.common.hash.Hashing;
import jersey.repackaged.com.google.common.hash.HashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.util.MurmurHash;
import org.apache.hadoop.hbase.util.MurmurHash3;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Slf4j
public class ZKDataUtil {

    public static final String BASE_PATH = "/hbase-compactor";
    public static final String CONTEXT_PATH = "/contexts";
    public static final String PROFILE_PATH = "/profiles";
    public static final String CONTEXT_DELIMITER = "-";

    private static final ObjectMapper serDeserManager = new ObjectMapper();

    public static final int DEFAULT_SESSION_TIMEOUT = 10000;
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
        String path = BASE_PATH + PROFILE_PATH + "/"+ profileID;
        log.info("creating path for compaction profile: {}", path);
        return path;
    }

    public static String getContextPath (CompactionContext compactionContext) {
        String key = compactionContext.getClusterID() + CONTEXT_DELIMITER +compactionContext.getRsGroup() +
                CONTEXT_DELIMITER + compactionContext.getNameSpace() + CONTEXT_DELIMITER + compactionContext.getTableName();
        String hashCode = Hashing.murmur3_32().hashString(key, StandardCharsets.UTF_8).toString();
        String path = BASE_PATH + CONTEXT_PATH + "/"+ hashCode;
        log.info("creating path for compaction context: {}", path);
        return path;
    }
}
