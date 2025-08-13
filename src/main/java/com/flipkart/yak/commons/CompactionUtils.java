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
        CompactionContext compactionContext = new CompactionContext(promptCompactionRequest.getClusterID(), new CompactionSchedule(startHour, endHour), promptCompactionRequest.getCompactionProfileID());
        compactionContext.setNameSpace(promptCompactionRequest.getNameSpace());
        compactionContext.setTableNames(promptCompactionRequest.getTableNames());
        compactionContext.setRsGroup(promptCompactionRequest.getRsGroup());
        compactionContext.getCompactionSchedule().setPrompt(true);
        compactionContext.getCompactionSchedule().setPromptCompactionLifespan(new PromptCompactionLifeSpan(lifeSpanStart, lifeSpanEnd));

        return compactionContext;
    }
}
