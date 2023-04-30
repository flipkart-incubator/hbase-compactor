package com.flipkart.yak.config.k8s;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.NonNull;

public class K8sConfig {
    /*
        Namespace for which watch needs to be setup
     */
    @NonNull String namespace;
    /*
        Watcher session read time. after this timeout, watcher will be reset. Higher value avoid frequent resets.
     */
    int readTimeout = 10000;
    /*
        Watcher connection setup timeout.
     */
    int connectionTimeout = 10000;
    /*
        If specified, i.e. non-null, system property KUBECONFIG will be set, from there serviceaccount details will be
        picked up.
     */
    String kubeConfigEnvironmentValue = null;

    @JsonProperty
    public int getReadTimeout() {
        return readTimeout;
    }
    @JsonProperty
    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }
    @JsonProperty
    public int getConnectionTimeout() {
        return connectionTimeout;
    }
    @JsonProperty
    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }
    @JsonProperty
    public String getKubeConfigEnvironmentValue() {
        return kubeConfigEnvironmentValue;
    }
    @JsonProperty
    public void setKubeConfigEnvironmentValue(String kubeConfigEnvironmentValue) {
        this.kubeConfigEnvironmentValue = kubeConfigEnvironmentValue;
    }
    @JsonProperty
    public String getNamespace() {
        return namespace;
    }
    @JsonProperty
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
