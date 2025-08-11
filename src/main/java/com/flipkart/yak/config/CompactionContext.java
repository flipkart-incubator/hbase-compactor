package com.flipkart.yak.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.yak.interfaces.Validable;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;


@Data
@SuperBuilder
@RequiredArgsConstructor
@Jacksonized
@Slf4j
public class CompactionContext implements Validable {


    @NonNull final String clusterID;

    @NonNull final CompactionSchedule compactionSchedule;

    String nameSpace = "default";

    String rsGroup = "default";

    @NonNull  final String compactionProfileID;

    String tableNames;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompactionContext)) return false;
        CompactionContext that = (CompactionContext) o;
        boolean isTargetSame = true;
        if (rsGroup != null && that.getRsGroup() != null && !rsGroup.equals(that.getRsGroup())) {
            isTargetSame = false;
        }
        if (nameSpace != null && that.getNameSpace() != null && !nameSpace.equals(that.getNameSpace())) {
            isTargetSame = false;
        }
        if (tableNames != null && that.getTableNames() != null && !tableNames.equals(that.getTableNames())) {
            isTargetSame = false;
        }
        return getClusterID().equals(that.getClusterID()) &&
                getCompactionProfileID().equals(that.getCompactionProfileID()) &&
                getCompactionSchedule().equals(that.getCompactionSchedule()) && isTargetSame;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClusterID(), getCompactionProfileID());
    }

    @Override
    public void validate() throws ConfigurationException {
        log.info("Validating CompactionContext: tableNames={}, nameSpace={}", tableNames, nameSpace);
        validateTable(nameSpace, rsGroup, tableNames);
    }

    static void validateTable(String nameSpace, String rsGroup, String tableNames) throws ConfigurationException {
        if (nameSpace == null && rsGroup == null && tableNames == null) {
            throw new ConfigurationException("no target for compaction specified");
        }
        if (tableNames != null) {
            for (String table : tableNames.split(",")) {
                if (table.contains(":")) {
                    String namespace = table.split(":")[0];
                    if (namespace.equals("hbase")) {
                        throw new ConfigurationException("hbase tables should not be compacted with custom trigger");
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return "{" +
                "clusterID:'" + clusterID + '\'' +
                ", nameSpace='" + nameSpace + '\'' +
                ", rsGroup:'" + rsGroup + '\'' +
                ", schedule:'" + compactionSchedule + '\''+
                ", compactionProfileID:'" + compactionProfileID + '\'' +
                ", tableNames:'" + tableNames + '\'' +
                '}';
    }
}
