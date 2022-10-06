package com.flipkart.yak.interfaces;


import org.apache.commons.configuration.ConfigurationException;

public interface Validable {
    void validate() throws ConfigurationException;
}
