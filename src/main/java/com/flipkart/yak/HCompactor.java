package com.flipkart.yak;

import com.flipkart.yak.config.CompactionConfigManger;
import com.flipkart.yak.core.CompactionRuntimeException;
import com.flipkart.yak.core.JobSubmitter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;

@Slf4j
public class HCompactor {

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "LOCAL_USER_NAME");
        try {
            (new JobSubmitter(CompactionConfigManger.get())).start();
        } catch (ConfigurationException | CompactionRuntimeException e) {
            log.error("could not load config, error:{} \n exiting ...", e.getMessage());
        }
    }
}
