package com.flipkart.yak.config.k8s;

import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionProfileConfig;
import com.flipkart.yak.config.loader.AbstractConfigWriter;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import io.kubernetes.client.util.PatchUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.hbase.util.Pair;

import java.security.KeyStore;
import java.util.HashMap;
import java.util.Objects;

/**
 * Implementation of {@link AbstractConfigWriter} to write ConfigMaps onto K8s
 */
@Slf4j
public class K8sConfigWriter extends AbstractConfigWriter<CoreV1Api> {
    static final String API_VERSION = "v1";
    static final String STORE_DATA_STRUCTURE_KIND = "ConfigMap";
    @Override
    public CoreV1Api init(String resourceName) throws ConfigurationException {
        return K8sUtils.init(resourceName);
    }

    /*
        Updating an existing resource consists of sending a PATCH request to the Kubernetes API server, containing which
        fields we want to modify. This one creates and executes configMap update by patching.
     */
    private void patch(V1ConfigMap newConfigMap, CoreV1Api coreV1Api, String fields) throws ApiException {
        Objects.requireNonNull(newConfigMap.getMetadata()).setManagedFields(null);
        newConfigMap.getMetadata().setResourceVersion(null);
        newConfigMap.setApiVersion(API_VERSION);
        newConfigMap.setKind(STORE_DATA_STRUCTURE_KIND);
        String patchedJson = coreV1Api.getApiClient().getJSON().serialize(newConfigMap);
        PatchUtils.patch(V1ConfigMap.class, () -> coreV1Api.patchNamespacedConfigMapCall(
                newConfigMap.getMetadata().getName(),
                newConfigMap.getMetadata().getNamespace(),
                new V1Patch(patchedJson),
                null, null, fields, false, null), V1Patch.PATCH_FORMAT_APPLY_YAML, coreV1Api.getApiClient());

    }

    @Override
    public boolean storeProfile(CoreV1Api coreV1Api, CompactionProfileConfig compactionProfileConfig) {
        String fields = K8sUtils.getFieldSelectorForProfile();
        V1ConfigMapList configMapList = null;
        synchronized (this) {
            try {
                configMapList = K8sUtils.execute(fields, coreV1Api);
                if( configMapList!= null && configMapList.getItems().size()>0) {
                    V1ConfigMap v1ConfigMap = configMapList.getItems().get(0);
                    Pair<String,String> profileConfigSer = K8sUtils.getSerializedProfiles(compactionProfileConfig);
                    if(v1ConfigMap.getData() == null) {
                        v1ConfigMap.setData(new HashMap<>());
                    }
                    v1ConfigMap.getData().put(profileConfigSer.getFirst(), profileConfigSer.getSecond());
                    this.patch(v1ConfigMap, coreV1Api, fields);
                    return true;
                }
            } catch (ConfigurationException | ApiException e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    @Override
    public boolean storeContext(CoreV1Api coreV1Api, CompactionContext compactionContext) {
        String fields = K8sUtils.getFieldSelectorForContext();
        V1ConfigMapList configMapList = null;
        synchronized (this) {
            try {
                configMapList = K8sUtils.execute(fields, coreV1Api);
                if( configMapList!= null && configMapList.getItems().size()>0) {
                    V1ConfigMap v1ConfigMap = configMapList.getItems().get(0);
                    Pair<String,String> profileConfigSer = K8sUtils.getSerializedContext(compactionContext);
                    if(v1ConfigMap.getData() == null) {
                        v1ConfigMap.setData(new HashMap<>());
                    }
                    v1ConfigMap.getData().put(profileConfigSer.getFirst(), profileConfigSer.getSecond());
                    this.patch(v1ConfigMap, coreV1Api, fields);
                    return true;
                }
            } catch (ConfigurationException | ApiException e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    @Override
    public boolean deleteContext(CoreV1Api coreV1Api, CompactionContext compactionContext) {
        String fields = K8sUtils.getFieldSelectorForContext();
        V1ConfigMapList configMapList = null;
        synchronized (this) {
            try {
                configMapList = K8sUtils.execute(fields, coreV1Api);
                if( configMapList!= null && configMapList.getItems().size()>0) {
                    V1ConfigMap v1ConfigMap = configMapList.getItems().get(0);
                    Pair<String,String> contextConfigSer = K8sUtils.getSerializedContext(compactionContext);
                    if(v1ConfigMap.getData() == null) {
                        v1ConfigMap.setData(new HashMap<>());
                    }
                    v1ConfigMap.getData().remove(contextConfigSer.getFirst());
                    this.patch(v1ConfigMap, coreV1Api, fields);
                    return true;
                }
            } catch (ConfigurationException | ApiException e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    @Override
    public boolean deleteStaleContexts(CoreV1Api coreV1Api) {
        String fields = K8sUtils.getFieldSelectorForContext();
        V1ConfigMapList configMapList = null;
        synchronized (this) {
            try {
                configMapList = K8sUtils.execute(fields, coreV1Api);
                if( configMapList!= null && configMapList.getItems().size()>0) {
                    V1ConfigMap v1ConfigMap = configMapList.getItems().get(0);
                    v1ConfigMap.getData().forEach((contextKey, contextValue) -> {
                        if (contextKey.endsWith("prompt")) {
                            try {
                                v1ConfigMap.getData().remove(contextKey);
                                log.info("Deleted prompt context {}", contextKey);
                                this.patch(v1ConfigMap, coreV1Api, fields);
                            }
                            catch (ApiException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                    return true;
                }
            } catch (ConfigurationException e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }
}
