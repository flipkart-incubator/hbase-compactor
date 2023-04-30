package com.flipkart.yak.config.k8s;

import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfileConfig;
import com.flipkart.yak.config.loader.AbstractConfigLoader;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link AbstractConfigLoader} with {@link CoreV1Api} as resource. Reads configMap data from store.
 */
@Slf4j
public class K8sConfigLoader extends AbstractConfigLoader<CoreV1Api> {
    @Override
    public CoreV1Api preCheckAndLoad(String resourceName) throws ConfigurationException {
        return K8sUtils.init(resourceName);
    }


    @Override
    public List<CompactionProfileConfig> getProfiles(CoreV1Api coreV1Api) throws ConfigurationException {
        List<CompactionProfileConfig> response = new ArrayList<>();
        String fields = K8sUtils.getFieldSelectorForProfile();
        V1ConfigMapList configMapList = null;
        synchronized (this) {
            configMapList = K8sUtils.execute(fields, coreV1Api);
        }
        if( configMapList!= null && configMapList.getItems().size()>0) {
            V1ConfigMap configMap = configMapList.getItems().get(0);
            if(configMap.getData()!= null) {
                for (Map.Entry<String, String> profile : configMap.getData().entrySet()) {
                    CompactionProfileConfig data = K8sUtils.getProfile(profile.getValue());
                    response.add(data);
                }
            }
        }
        return response;
    }

    @Override
    public List<CompactionContext> getCompactionContexts(CoreV1Api coreV1Api) throws ConfigurationException {
        List<CompactionContext> response = new ArrayList<>();
        String fields = K8sUtils.getFieldSelectorForContext();
        V1ConfigMapList configMapList = null;
        synchronized (this) {
            configMapList = K8sUtils.execute(fields, coreV1Api);
        }
        if( configMapList!= null && configMapList.getItems().size()>0) {
            log.info("received data from store: {}", configMapList.getItems().size());
            V1ConfigMap configMap = configMapList.getItems().get(0);
            if(configMap.getData()!= null) {
                for (Map.Entry<String, String> profile : configMap.getData().entrySet()) {
                    log.info("processing {}", profile.getKey());
                    CompactionContext data = K8sUtils.getContext(profile.getValue());
                    response.add(data);
                }
            }
        }
        return response;
    }

    @Override
    protected void close(CoreV1Api resourceType) {

    }
}
