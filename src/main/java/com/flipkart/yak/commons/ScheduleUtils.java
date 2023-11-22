package com.flipkart.yak.commons;

import com.flipkart.yak.config.CompactionSchedule;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.DateUtils;

import java.time.Instant;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Date;

@Slf4j
public class ScheduleUtils {

    public static long  getStartOfTheDay(Instant instant) {
        Calendar calendar = DateUtils.toCalendar(Date.from(instant));
        Calendar todaysDate = DateUtils.truncate(calendar, Calendar.DATE);
        return todaysDate.getTimeInMillis();
    }

    public static float getCurrentHour() {
        return LocalTime.now().getHour();
    }

    public static float getEndHour(float duration) {
        float currentHour = getCurrentHour();
        float endHour = currentHour + duration;
        return endHour;
    }

    public static long getCurrentTimeInEpochMilli() {
        return Instant.now().toEpochMilli();
    }

    public static long getEndTimeInEpochMilli(float duration) {
        long baseTime =  getCurrentTimeInEpochMilli();
        long endTime = baseTime + (long) (duration * DateUtils.MILLIS_PER_HOUR);
        return endTime;
    }

    public static long getSleepTime(CompactionSchedule compactionSchedule) {
        return getSleepTime(compactionSchedule, Instant.now());
    }

    public static long getSleepTime(CompactionSchedule compactionSchedule, Instant instant) {
        long baseTime = getStartOfTheDay(instant);
        long startTimeToday = baseTime + (long)(compactionSchedule.getStartHourOfTheDay() * DateUtils.MILLIS_PER_HOUR);
        long startTimeTomorrow = startTimeToday + DateUtils.MILLIS_PER_DAY;
        long currTime = instant.toEpochMilli();
        if (currTime > startTimeToday) {
            log.debug("calculated thread sleep time {}", (startTimeTomorrow-currTime));
            return startTimeTomorrow-currTime;
        }
        log.debug("calculated thread sleep time {}", (startTimeToday-currTime));
        return startTimeToday - currTime;
    }

    public static boolean hasTimedOut(CompactionSchedule compactionSchedule) {
        return hasTimedOut(compactionSchedule, Instant.now());
    }

    public static boolean hasTimedOut(CompactionSchedule compactionSchedule, Instant instant) {
        long baseTime = getStartOfTheDay(instant);
        long currTIme = instant.toEpochMilli();
        long endTime = baseTime + (long)(compactionSchedule.getEndHourOfTheDay() * DateUtils.MILLIS_PER_HOUR);
        if (currTIme < endTime) {
            return false;
        }
        return true;
    }

    public static boolean canStart(CompactionSchedule compactionSchedule) {
        return canStart(compactionSchedule, Instant.now());
    }

    public static boolean canStart(CompactionSchedule compactionSchedule, Instant instant) {
        long baseTime = getStartOfTheDay(instant);
        long currTime = instant.toEpochMilli();
        long startTime = baseTime + (long)(compactionSchedule.getStartHourOfTheDay() * DateUtils.MILLIS_PER_HOUR);
        if (currTime >= startTime) {
            return true;
        }
        return false;
    }

    public static boolean hasExpired(CompactionSchedule compactionSchedule, Instant instant) {
        long lifeCycleEndTime = compactionSchedule.getPromptCompactionLifespan().getEndSpan();
        long currTime = instant.now().toEpochMilli();
        if (currTime > lifeCycleEndTime) {
            return true;
        }
        return false;
    }

}
