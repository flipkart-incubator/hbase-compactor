package com.flipkart.yak.config;


import com.flipkart.yak.interfaces.Validable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;

import java.util.HashSet;
import java.util.Set;

@Getter
@Slf4j
public class CompactionTriggerConfig {

    Set<CompactionContext> compactionContexts;
    Set<CompactionProfileConfig> compactionProfileConfigs;

    private CompactionTriggerConfig(){};

    public static class Builder {

        Set<CompactionContext> compactionContext = new HashSet<>();
        Set<CompactionProfileConfig> compactionProfileConfig = new HashSet<>();

        public Builder withCompactionContexts(Set<CompactionContext> compactionContexts) {
            compactionContexts.forEach(this::withCompactionContext);
            return this;
        }

        public Builder withCompactionProfiles(Set<CompactionProfileConfig> compactionProfileConfigs) {
            compactionProfileConfigs.forEach(this::withCompactionProfile);
            return this;
        }

        public Builder withCompactionContext(CompactionContext context) {
            try {
                validateContext(context);
                compactionContext.add(context);
            } catch (ConfigurationException e) {
                log.error("Compaction Context Can not be validated, ignoring {} error : {}", context, e.getMessage());
            }
            return this;
        }

        public Builder withCompactionProfile(CompactionProfileConfig profile) {
            try {
                validateProfile(profile);
                compactionProfileConfig.add(profile);
            } catch (ConfigurationException e) {
                log.error("Compaction Profile can not be validated, ignoring {} error: {} ", profile, e.getMessage());
            }
            return this;
        }

        private void validateProfile(Validable cpr) throws ConfigurationException {
            cpr.validate();
        }

        private void validateContext(Validable ctx) throws ConfigurationException {
            ctx.validate();
        }


        public CompactionTriggerConfig build() {
            CompactionTriggerConfig compactionTriggerConfig = new CompactionTriggerConfig();
            compactionTriggerConfig.compactionContexts = this.compactionContext;
            compactionTriggerConfig.compactionProfileConfigs = this.compactionProfileConfig;
            return compactionTriggerConfig;
        }
    }

}
