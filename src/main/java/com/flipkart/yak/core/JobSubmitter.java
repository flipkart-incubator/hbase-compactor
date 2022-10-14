package com.flipkart.yak.core;

import com.flipkart.yak.config.CompactionTriggerConfig;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class JobSubmitter {

    CompactionTriggerConfig compactionTriggerConfig;


    public JobSubmitter(CompactionTriggerConfig compactionTriggerConfig) {
        this.compactionTriggerConfig = compactionTriggerConfig;
    }

    public void start() {

    }
}
