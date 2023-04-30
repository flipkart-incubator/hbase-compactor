package com.flipkart.yak.rest;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.yak.config.k8s.K8sConfig;
import com.flipkart.yak.config.zkstore.ZKConfig;
import io.dropwizard.Configuration;
import lombok.NonNull;

import javax.validation.constraints.NotNull;


public class AppConfig extends Configuration {

    ZKConfig zkConfig = null;

    @NonNull
    String hadoopUserName;

    K8sConfig k8sConfig = null;
    @JsonProperty
    public K8sConfig getK8sConfig() {
        return k8sConfig;
    }
    @JsonProperty
    public void setK8sConfig(K8sConfig k8sConfig) {
        this.k8sConfig = k8sConfig;
    }

    @JsonProperty
    public String getHadoopUserName() {
        return hadoopUserName;
    }

    @JsonProperty
    public void setHadoopUserName(String hadoopUserName) {
        this.hadoopUserName = hadoopUserName;
    }

    @JsonProperty
    public ZKConfig getZkConfig() {
        return zkConfig;
    }

    @JsonProperty
    public void setZkConfig(ZKConfig zkConfig) {
        this.zkConfig = zkConfig;
    }
}
