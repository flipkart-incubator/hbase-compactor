package com.flipkart.yak;

import com.codahale.metrics.jmx.JmxReporter;
import com.flipkart.yak.config.StoreFactory;
import com.flipkart.yak.rest.AppConfig;
import com.flipkart.yak.rest.ConfigController;
import com.flipkart.yak.rest.DefaultHealthCheck;
import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

public class ConfigManager extends Application<AppConfig> {

    public static void main(String[] args) {
        try {
            new ConfigManager().run(args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void readAppConfig() {

    }

    @Override
    public void run(AppConfig appConfig, Environment environment) throws Exception {
        StoreFactory storeFactory = StoreFactory.getInstance();
        ConfigController configController = new ConfigController(storeFactory);
        environment.jersey().register(configController);
        environment.healthChecks().register("default", new DefaultHealthCheck());
    }
}
