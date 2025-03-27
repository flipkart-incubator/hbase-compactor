package com.flipkart.yak.commons;

import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionSchedule;
import com.flipkart.yak.config.PromptCompactionRequest;
import com.flipkart.yak.config.PromptCompactionLifeSpan;

public class CompactionUtils {

    public static CompactionContext getCompactionContext(PromptCompactionRequest promptCompactionRequest) {
        float startHour = ScheduleUtils.getCurrentHour();
        float endHour = ScheduleUtils.getEndHour(promptCompactionRequest.getDuration());
        long lifeSpanStart = ScheduleUtils.getCurrentTimeInEpochMilli();
        long lifeSpanEnd = ScheduleUtils.getEndTimeInEpochMilli(promptCompactionRequest.getDuration());
        CompactionSchedule compactionSchedule = new CompactionSchedule(startHour, endHour);
        compactionSchedule.setPrompt(true);
        compactionSchedule.setPromptCompactionLifespan(new PromptCompactionLifeSpan(lifeSpanStart, lifeSpanEnd));

        CompactionContext.CompactionContextBuilder builder = CompactionContext.builder()
                .clusterID(promptCompactionRequest.getClusterID())
                .compactionSchedule(compactionSchedule)
                .nameSpace(promptCompactionRequest.getNameSpace())
                .rsGroup(promptCompactionRequest.getRsGroup())
                .compactionProfileID(promptCompactionRequest.getCompactionProfileID());

        if (promptCompactionRequest.getTableNames() != null) {
            builder.tableNames(promptCompactionRequest.getTableNames());
        } else if (promptCompactionRequest.getTableName() != null) {
            builder.tableName(promptCompactionRequest.getTableName());
        }

        return builder.build();
    }
}
