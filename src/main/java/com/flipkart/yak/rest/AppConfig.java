package com.flipkart.yak.rest;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.yak.config.zkstore.ZKConfig;
import io.dropwizard.Configuration;
import lombok.NonNull;

import javax.validation.constraints.NotNull;


public class AppConfig extends Configuration {

    @NotNull
    ZKConfig zkConfig;

    @NonNull
    String hadoopUserName;

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
