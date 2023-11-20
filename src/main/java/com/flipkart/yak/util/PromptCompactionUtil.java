package com.flipkart.yak.util;

import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionSchedule;
import com.flipkart.yak.config.PromptCompactionRequest;
import com.flipkart.yak.config.PromptSchedule;

import static com.flipkart.yak.commons.ScheduleUtils.*;
import static com.flipkart.yak.commons.ScheduleUtils.getEndTimeInEpochMilli;

public class PromptCompactionUtil {

    public static CompactionContext getCompactionContext(PromptCompactionRequest promptCompactionRequest) {
        float startHour = getCurrentHour();
        float endHour = getEndHour(promptCompactionRequest.getDuration());
        long lifeCycleStartTime = getCurrentTimeInEpochMilli();
        long lifeCycleEndTime = getEndTimeInEpochMilli(endHour);
        CompactionContext compactionContext = new CompactionContext(promptCompactionRequest.getClusterID(), new CompactionSchedule(startHour, endHour), promptCompactionRequest.getCompactionProfileID());
        compactionContext.setNameSpace(promptCompactionRequest.getNameSpace());
        compactionContext.setTableName(promptCompactionRequest.getTableName());
        compactionContext.setRsGroup(promptCompactionRequest.getRsGroup());
        compactionContext.getCompactionSchedule().setPrompt(true);
        compactionContext.getCompactionSchedule().setPromptSchedule(new PromptSchedule(lifeCycleStartTime, lifeCycleEndTime));

        return compactionContext;
    }
}
