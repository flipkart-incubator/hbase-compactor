package com.flipkart.yak;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flipkart.yak.config.ConfigListener;
import com.flipkart.yak.interfaces.Factory;
import com.flipkart.yak.config.StoreFactory;
import com.flipkart.yak.core.CompactionRuntimeException;
import com.flipkart.yak.core.JobSubmitter;
import com.flipkart.yak.core.MonitorService;
import com.flipkart.yak.rest.AppConfig;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.logging.ExternalLoggingFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;

import java.io.File;


/**
 *  Driver class to initiate Compactor Tasks
 */
@Slf4j
public class HCompactor {

    private static String HADOOP_USER_NAME_KEY = "HADOOP_USER_NAME";
    private static String DEFAULT_CONFIG_FILE = "config.yml";

    public static void main(String[] args) {
        ObjectMapper objectMapper = Jackson.newObjectMapper(new YAMLFactory());
        try {
            AppConfig appConfig = objectMapper.readValue(new File(DEFAULT_CONFIG_FILE), AppConfig.class);
            appConfig.setLoggingFactory(new ExternalLoggingFactory());
            System.setProperty(HADOOP_USER_NAME_KEY, appConfig.getHadoopUserName());
            JobSubmitter taskSubmitter = new JobSubmitter();
            MonitorService.start();
            Factory factory = StoreFactory.getInstance();
            factory.init(appConfig.getZkConfig());
            ConfigListener configListener = factory.getConfigListener();
            configListener.register(taskSubmitter);
            configListener.listen();
        } catch (ConfigurationException | CompactionRuntimeException | RuntimeException e) {
            log.error("could not load config, error:{} \n exiting ...", e.getMessage());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
