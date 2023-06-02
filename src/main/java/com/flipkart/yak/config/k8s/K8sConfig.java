package com.flipkart.yak.config.k8s;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.NonNull;

import java.util.Map;

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
    /*
        If specified, i.e. non-null, system property ENV_SERVICE_HOST will be set.
     */
    String kubeApiHost = null;

    @JsonProperty
    public String getKubeApiHost() {
        return kubeApiHost;
    }

    @JsonProperty
    public void setKubeApiHost(String kubeApiHost) {
        this.kubeApiHost = kubeApiHost;
    }

    @JsonProperty
    public String getKubeApiPort() {
        return kubeApiPort;
    }

    @JsonProperty
    public void setKubeApiPort(String kubeApiPort) {
        this.kubeApiPort = kubeApiPort;
    }

    /*
            If specified, i.e. non-null, system property ENV_SERVICE_PORT will be set.
         */
    String kubeApiPort = null;

    /*
        Additional labels if required, If not specified only one label will be used.
     */
    @JsonDeserialize
    Map<String, String> additionalLabels = null;
    /*
        Additional annotations if required, If not set, no annotation will be applied.
     */
    @JsonDeserialize
    Map<String, String> additionalAnnotations = null;

    @JsonProperty
    public Map<String, String> getAdditionalLabels() {
        return additionalLabels;
    }

    @JsonProperty
    public void setAdditionalLabels(Map<String, String> additionalLabels) {
        this.additionalLabels = additionalLabels;
    }
    @JsonProperty
    public Map<String, String> getAdditionalAnnotations() {
        return additionalAnnotations;
    }
    @JsonProperty
    public void setAdditionalAnnotations(Map<String, String> additionalAnnotations) {
        this.additionalAnnotations = additionalAnnotations;
    }

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
