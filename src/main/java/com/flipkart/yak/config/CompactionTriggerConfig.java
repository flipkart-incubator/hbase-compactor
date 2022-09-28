package com.flipkart.yak.config;


import lombok.Getter;

import java.util.Set;

@Getter
public class CompactionTriggerConfig {

    Set<CompactionContext> compactionContexts;
    Set<CompactionProfile> compactionProfiles;

    public static class Builder {

        public CompactionTriggerConfig build() {
            return null;
        }
    }

}
