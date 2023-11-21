package com.flipkart.yak.util;

import com.flipkart.yak.commons.ScheduleUtils;
import com.flipkart.yak.config.CompactionContext;
import com.flipkart.yak.config.CompactionSchedule;
import com.flipkart.yak.config.PromptCompactionRequest;
import com.flipkart.yak.config.PromptSchedule;

public class PromptCompactionUtil {

    public static CompactionContext getCompactionContext(PromptCompactionRequest promptCompactionRequest) {
        float startHour = ScheduleUtils.getCurrentHour();
        float endHour = ScheduleUtils.getEndHour(promptCompactionRequest.getDuration());
        long lifeCycleStartTime = ScheduleUtils.getCurrentTimeInEpochMilli();
        long lifeCycleEndTime = ScheduleUtils.getEndTimeInEpochMilli(promptCompactionRequest.getDuration());
        CompactionContext compactionContext = new CompactionContext(promptCompactionRequest.getClusterID(), new CompactionSchedule(startHour, endHour), promptCompactionRequest.getCompactionProfileID());
        compactionContext.setNameSpace(promptCompactionRequest.getNameSpace());
        compactionContext.setTableName(promptCompactionRequest.getTableName());
        compactionContext.setRsGroup(promptCompactionRequest.getRsGroup());
        compactionContext.getCompactionSchedule().setPrompt(true);
        compactionContext.getCompactionSchedule().setPromptSchedule(new PromptSchedule(lifeCycleStartTime, lifeCycleEndTime));

        return compactionContext;
    }
}
