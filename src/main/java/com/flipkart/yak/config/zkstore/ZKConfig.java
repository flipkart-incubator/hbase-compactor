package com.flipkart.yak.config.zkstore;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.NonNull;

public class ZKConfig {
    @NonNull  String connectionString;
    boolean readOnly = false;

    @JsonProperty
    public String getConnectionString() {
        return connectionString;
    }

    @JsonProperty
    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    @JsonProperty
    public boolean isReadOnly() {
        return readOnly;
    }

    @JsonProperty
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }
}
