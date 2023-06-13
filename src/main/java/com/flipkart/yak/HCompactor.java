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
import java.net.URL;


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
            URL url = Thread.currentThread().getContextClassLoader().getResource(DEFAULT_CONFIG_FILE);
            if(url==null) {
                throw new ConfigurationException("Could not load config file");
            }
            AppConfig appConfig = objectMapper.readValue(new File(url.getPath()), AppConfig.class);
            appConfig.setLoggingFactory(new ExternalLoggingFactory());
            setHadoopUserNameKey(appConfig);
            JobSubmitter taskSubmitter = new JobSubmitter();
            MonitorService.start();
            StoreFactory.StoreFactoryBuilder builder = new StoreFactory.StoreFactoryBuilder();
            Factory factory = builder.withConfig(appConfig).getFactory();
            ConfigListener configListener = factory.getConfigListener();
            configListener.register(taskSubmitter);
            configListener.listen();
        } catch (ConfigurationException | CompactionRuntimeException | RuntimeException e) {
            log.error("could not load config, error:{} \n exiting ...", e.getMessage());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void setHadoopUserNameKey(AppConfig appConfig){
        if(System.getenv(HADOOP_USER_NAME_KEY) == null){
            log.info("No {} specified in Env, setting from config", HADOOP_USER_NAME_KEY);
            System.setProperty(HADOOP_USER_NAME_KEY, appConfig.getHadoopUserName());
        }
        else {
            log.info("Found {} in ENV, overriding value given in config", HADOOP_USER_NAME_KEY);
            System.setProperty(HADOOP_USER_NAME_KEY, System.getenv(HADOOP_USER_NAME_KEY));
        }
    }
}
