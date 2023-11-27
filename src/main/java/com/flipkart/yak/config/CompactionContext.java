package com.flipkart.yak.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.yak.interfaces.Validable;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.configuration.ConfigurationException;

import java.util.Objects;


@Data
@SuperBuilder
@RequiredArgsConstructor
@Jacksonized
public class CompactionContext implements Validable {


    @NonNull final String clusterID;

    @NonNull final CompactionSchedule compactionSchedule;

    String tableName;

    String nameSpace = "default";

    String rsGroup = "default";

    @NonNull  final String compactionProfileID;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompactionContext)) return false;
        CompactionContext that = (CompactionContext) o;
        boolean isTargetSame = true;
        if (rsGroup != null && that.getRsGroup()!= null && !rsGroup.equals(that.getRsGroup())) {
            isTargetSame = false;
        }
        if (nameSpace != null && that.getNameSpace()!= null && !nameSpace.equals(that.getNameSpace())) {
            isTargetSame = false;
        }
        if (tableName != null && that.getTableName()!= null && !tableName.equals(that.getTableName())) {
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
        validateTable(tableName, nameSpace, rsGroup);
    }

    static void validateTable(String tableName, String nameSpace, String rsGroup) throws ConfigurationException {
        if (tableName == null && nameSpace == null && rsGroup == null) {
            throw new ConfigurationException("no target for compaction specified");
        }
        if (tableName != null && tableName.contains(":")) {
            String namespace = tableName.split(":")[0];
            if (namespace.equals("hbase")) {
                throw new ConfigurationException("hbase tables should not be compacted with custom trigger");
            }
        }
    }

    @Override
    public String toString() {
        return "{" +
                "clusterID:'" + clusterID + '\'' +
                ", tableName:'" + tableName + '\'' +
                ", nameSpace='" + nameSpace + '\'' +
                ", rsGroup:'" + rsGroup + '\'' +
                ", schedule:'" + compactionSchedule + '\''+
                ", compactionProfileID:'" + compactionProfileID + '\'' +
                '}';
    }
}
