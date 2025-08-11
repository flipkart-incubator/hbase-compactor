package com.flipkart.yak.config;

import com.flipkart.yak.interfaces.Validable;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.configuration.ConfigurationException;

@Data
@SuperBuilder
@RequiredArgsConstructor
@Jacksonized
public class PromptCompactionRequest implements Validable {

    @NonNull final String clusterID;

    float duration;

    String nameSpace = "default";

    String rsGroup = "default";

    @NonNull  final String compactionProfileID;

    String tableNames;

    @Override
    public void validate() throws ConfigurationException {

        CompactionContext.validateTable(nameSpace, rsGroup, tableNames);
        if (duration == 0) {
            throw new ConfigurationException("duration cannot be null");
        }

    }
}
