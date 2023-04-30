package com.flipkart.yak.config.k8s;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfileConfig;
import com.google.common.hash.Hashing;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class K8sUtils {
    private static final String PROFILE_CONFIGMAP_NAME = "compaction-trigger-profiles";
    private static final String CONTEXT_CONFIGMAP_NAME = "compaction-trigger-context";
    private static final String APP_NAME_LABEL = "hbase-compactor";
    private static final Map<String, String> labels = new HashMap<>();
    private static String namespace;

    private static CoreV1Api api;
    private static ApiClient client;

    private static final ObjectMapper serDeserManager = new ObjectMapper(new YAMLFactory());

    public static String getNamespace(CoreV1Api coreV1Api) {
        return namespace;
    }

    public static String getLabels() {
        return "compactor.app/name="+APP_NAME_LABEL;
    }

    public static String getFieldSelectorForProfile() {
        return "metadata.name="+PROFILE_CONFIGMAP_NAME;
    }

    public static String getFieldSelectorForContext() {
        return "metadata.name="+CONTEXT_CONFIGMAP_NAME;
    }

    public static CompactionProfileConfig getProfile(String rawData) {
        try {
            return serDeserManager.readValue(rawData, CompactionProfileConfig.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static CompactionContext getContext(String rawData) {
        try {
            return serDeserManager.readValue(rawData, CompactionContext.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static CoreV1Api init(String resource) throws ConfigurationException {
        namespace = resource;
        if(api!=null) {
            return api;
        }
        try {
            client = Config.defaultClient();
        } catch (IOException e) {
            throw new ConfigurationException(e);
        }
        api = new CoreV1Api(client);
        initBaseConfigMap(resource);
        return api;
    }

    public static ApiClient getClient() {
        return client;
    }

    public static CoreV1Api getApi() {
        return api;
    }

    public static boolean initBaseConfigMap(String resource) throws ConfigurationException {
        return createBaseProfileConfigMap(resource);
    }

    private static boolean checkIfConfigMapExists(V1ConfigMapList v1ConfigMapList, String name){
        if(v1ConfigMapList == null) {
            return false;
        }
        for(V1ConfigMap data: v1ConfigMapList.getItems()) {
            if(data.getMetadata().getName().equals(CONTEXT_CONFIGMAP_NAME)) {
                return true;
            }
        }
        return false;
    }


    private static boolean createBaseProfileConfigMap(String namespace) {
        V1ConfigMapList v1ConfigMapList = null;
        try {
            v1ConfigMapList = K8sUtils.execute(null,api);
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
        V1ConfigMap v1ProfileConfigMap = getBasicV1ConfigMap(namespace);
        V1ConfigMap v1ContextConfigMap = getBasicV1ConfigMap(namespace);
        v1ProfileConfigMap.getMetadata().setName(PROFILE_CONFIGMAP_NAME);
        v1ContextConfigMap.getMetadata().setName(CONTEXT_CONFIGMAP_NAME);
        try {
            if(!checkIfConfigMapExists(v1ConfigMapList, PROFILE_CONFIGMAP_NAME)) {
                V1ConfigMap configMapProfile = api.createNamespacedConfigMap(namespace,v1ProfileConfigMap, "true",
                        null, null);
                if(configMapProfile == null) {
                    return false;
                }
            }
            if(!checkIfConfigMapExists(v1ConfigMapList, CONTEXT_CONFIGMAP_NAME)) {
                V1ConfigMap configMapContext = api.createNamespacedConfigMap(namespace,v1ContextConfigMap, "true",
                        null, null);
                if( configMapContext == null) {
                    return false;
                }
            }
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    private static V1ConfigMap getBasicV1ConfigMap(String namespace) {
        Map<String, String>  label = getLabelsMap();
        Map<String, String> data = new HashMap<>();
        V1ConfigMap v1ConfigMap = new V1ConfigMap();
        V1ObjectMeta v1ObjectMetaForConfigMap = new V1ObjectMeta();
        v1ObjectMetaForConfigMap.setNamespace(namespace);
        v1ObjectMetaForConfigMap.setLabels(label);
        v1ConfigMap.setData(data);
        v1ConfigMap.setMetadata(v1ObjectMetaForConfigMap);
        return v1ConfigMap;
    }

    private static Map<String, String> getLabelsMap() {
        labels.put("compactor.app/name",APP_NAME_LABEL);
        return labels;
    }

    public static Pair<String, String> getSerializedContext(CompactionContext compactionContext){
        String key = compactionContext.getClusterID() +compactionContext.getRsGroup() +
                  compactionContext.getNameSpace() + compactionContext.getTableName();
        String hashCode = Hashing.murmur3_32().hashString(key, StandardCharsets.UTF_8).toString();
        String value = null;
        try {
            value = serDeserManager.writeValueAsString(compactionContext);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        Pair<String, String> data = new Pair<>(hashCode, value);
        return data;
    }

    public static Pair<String, String> getSerializedProfiles(CompactionProfileConfig compactionProfileConfig){
        String key = compactionProfileConfig.getID();
        String value = null;
        try {
            value = serDeserManager.writeValueAsString(compactionProfileConfig);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        Pair<String, String> data = new Pair<>(key, value);
        return data;
    }

    /*
        Method to execute listNamespacedConfigMap from CoreV1Api to read configMap
        //TODO: This should be moved out of Util, and should be part of Handler class which is to be passed to calling DAL
     */
    public static V1ConfigMapList execute(String fields, CoreV1Api coreV1Api) throws ConfigurationException {
        String namespace = K8sUtils.getNamespace(coreV1Api);
        String labels = K8sUtils.getLabels();
        V1ConfigMapList configMapList= null;
        try {
            configMapList = coreV1Api.listNamespacedConfigMap(namespace, null,false, null, fields, labels,
                    1, null, null, 5 , false);
        } catch (ApiException e) {
            log.error("Could not read profiles: {}", e.getMessage());
            throw new ConfigurationException(e);
        }
        return configMapList;
    }
}
