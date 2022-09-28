package com.flipkart.yak.config;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Objects;

@Getter @Setter
@RequiredArgsConstructor
public class CompactionContext {

    @NonNull final String clusterID;
    @NonNull final CompactionSchedule compactionSchedule;
    String tableName;
    String nameSpace;
    String rsGroup;
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
}
