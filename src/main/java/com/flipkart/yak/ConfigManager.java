package com.flipkart.yak;

import com.flipkart.yak.interfaces.Factory;
import com.flipkart.yak.config.StoreFactory;
import com.flipkart.yak.rest.AppConfig;
import com.flipkart.yak.rest.ConfigController;
import com.flipkart.yak.rest.DefaultHealthCheck;
import io.dropwizard.Application;
import io.dropwizard.setup.Environment;


/**
 * REST API controller for storing and maintaining config.
 */
public class ConfigManager extends Application<AppConfig> {

    public static void main(String[] args) {
        try {
            new ConfigManager().run(args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run(AppConfig appConfig, Environment environment) throws Exception {
        Factory storeFactory = StoreFactory.getInstance();
        storeFactory.init(appConfig.getK8sConfig());
        ConfigController configController = new ConfigController(storeFactory, appConfig.getK8sConfig().getNamespace());
        environment.jersey().register(configController);
        environment.healthChecks().register("default", new DefaultHealthCheck());
    }
}
