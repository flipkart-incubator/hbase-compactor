package com.flipkart.yak.config.k8s;

import com.flipkart.yak.config.CompactionProfileConfig;
import com.flipkart.yak.config.ConfigListener;
import com.flipkart.yak.config.loader.AbstractConfigLoader;
import com.flipkart.yak.core.CompactionRuntimeException;
import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import io.kubernetes.client.util.Watch;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import org.apache.commons.configuration.ConfigurationException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * {@link ConfigListener} implementation for k8s api. Uses field-selector and label-selector to identify the configMaps
 * to watch on.
 */
@Slf4j
public class K8sConfigListener extends ConfigListener {
    private final CoreV1Api api;
    private final ApiClient client;

    private final String namespace;
    private final String label;


    public K8sConfigListener(AbstractConfigLoader configLoader, CoreV1Api api, ApiClient client, String namespace) {
        super(configLoader);
        this.api = api;
        this.client = client;
        this.namespace = namespace;
        this.label = K8sUtils.getLabels();
    }

    /*
        Creates two different Watcher thread for Contexts and Profiles respectively. It was a design choice to create
        separate configMaps for profiles and contexts for better readability and isolation, hence two separate threads
        are required to monitor the changes.
     */
    @Override
    public void listen() {
        ChangeWatcher profileWatcher = new ChangeWatcher(this.namespace, K8sUtils.getFieldSelectorForProfile(), this);
        ChangeWatcher contextWatcher = new ChangeWatcher(this.namespace, K8sUtils.getFieldSelectorForContext(), this);
        ExecutorService watchers = Executors.newFixedThreadPool(2);
        Future profile = watchers.submit(profileWatcher);
        Future context = watchers.submit(contextWatcher);
        watchers.shutdown();
    }

    /**
     * Watcher threads that watches on ConfigMap changes.
     */
    public class ChangeWatcher implements Runnable {

        /*
            This setup is required to remove reloading and acknowledging all the changes from t0. By default, upon starting,
            K8s api returns all the events from the first setup time, if resourceVersion is set up us null. So, lastProcessedVersion
            stores the already processes version, to avoid re-processing of already acknowledged changes.
         */
        private String currentResourceVersion = null;
        private String lastProcessedVersion = null;
        private final String namespace;
        private final String fieldSelector;
        private final ConfigListener handler;
        public ChangeWatcher(String namespace, String fieldSelector, ConfigListener handler) {
            this.namespace = namespace;
            this.fieldSelector = fieldSelector;
            this.handler = handler;
        }

        @Override
        public void run() {
            log.info("Staring watch for {}", fieldSelector);
            while (true) {
                try {
                    Watch<V1ConfigMap> currentWatcher = this.setupWatcher();
                    this.awaitEvent(currentWatcher);
                } catch (ApiException e) {
                    log.error("Exception during watch: {}", e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }
        /*
            This sets-up watcher on configMap.
            //TODO: timeout should be config driven
         */
        private Watch<V1ConfigMap> setupWatcher() throws ApiException {
            Call call = api.listNamespacedConfigMapCall(this.namespace, null, false , null,
                    fieldSelector, label, 2, currentResourceVersion, null, 120, true, null);
            return Watch.createWatch(client, call, new TypeToken<Watch.Response<V1ConfigMap>>(){}.getType());
        }

        /*
            Awaits the thread until the timeout expires. Upon expiry, it raises an exception with code 410, which is handled
            and Watcher is set again.
         */
        private void awaitEvent(Watch<V1ConfigMap> v1ConfigMapWatch) {
            try {
                while (v1ConfigMapWatch.hasNext()) {
                    Watch.Response<V1ConfigMap> response = v1ConfigMapWatch.next();
                    if (response == null) {
                        break;
                    }
                    switch (response.type) {
                        case "ADDED":
                        case "MODIFIED":
                        case "DELETED":
                            currentResourceVersion = this.getLatestResourceVersion();
                            if (lastProcessedVersion == null || !currentResourceVersion.equals(lastProcessedVersion)) {
                                log.debug("Valid Event Detected: {}, Awaiting for version more than : {}", response.type, currentResourceVersion);
                                lastProcessedVersion = currentResourceVersion;
                                this.handler.onChange();
                            }
                            break;
                        default:
                            if (response.status.getCode() == 410) {
                                currentResourceVersion = null;
                            }
                            log.warn("Received Unhandled Event: {}: {}", response.type, response.status.getMessage());
                    }
                }
            } catch (Throwable e) {
                log.warn(e.getMessage());
            }
        }

        private String getLatestResourceVersion() {
            String resourceVersion = null;
            V1ConfigMapList configMapList = null;
            synchronized (this) {
                try {
                    configMapList = K8sUtils.execute(fieldSelector, api);
                } catch (ConfigurationException e) {
                    log.error("could not read configmap to get latest version");
                    return null;
                }
            }
            if(configMapList!=null && configMapList.getItems().size() > 0) {
                V1ConfigMap configMap = configMapList.getItems().get(0);
                if(configMap.getData()!= null) {
                    resourceVersion = configMap.getMetadata().getResourceVersion();
                }
            }
            return resourceVersion;
        }
    }
}
