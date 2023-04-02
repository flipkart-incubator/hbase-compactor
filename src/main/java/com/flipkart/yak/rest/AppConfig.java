package com.flipkart.yak.rest;


import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

import javax.validation.constraints.NotNull;


public class AppConfig extends Configuration {

    @NotNull
    String zookeeperHost;


    @JsonProperty
    public String getZookeeperHost() {
        return zookeeperHost;
    }
    @JsonProperty
    public void setZookeeperHost(String zookeeperHost) {
        this.zookeeperHost = zookeeperHost;
    }

}
