package com.flipkart.yak.core;

import com.flipkart.yak.config.CompactionSchedule;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.DateUtils;

import java.time.Instant;
import java.util.Calendar;
import java.util.Date;

@Slf4j
public class ScheduleUtils {

    public static long  getStartOfTheDay() {
        Calendar calendar = DateUtils.toCalendar(Date.from(Instant.now()));
        Calendar todaysDate = DateUtils.truncate(calendar, Calendar.DATE);
        return todaysDate.getTimeInMillis();
    }

    public static long getSleepTime(CompactionSchedule compactionSchedule) {
        long baseTime = getStartOfTheDay();
        long startTimeToday = baseTime + (long)(compactionSchedule.getStartHourOfTheDay() * DateUtils.MILLIS_PER_HOUR);
        long startTimeTomorrow = startTimeToday + DateUtils.MILLIS_PER_DAY;
        long currTime = System.currentTimeMillis();
        if (currTime > startTimeToday) {
            log.debug("calculated thread sleep time {}", (startTimeTomorrow-currTime));
            return startTimeTomorrow-currTime;
        }
        log.debug("calculated thread sleep time {}", (startTimeToday-currTime));
        return startTimeToday - currTime;
    }

    public static boolean hasTimedOut(CompactionSchedule compactionSchedule) {
        long baseTime = getStartOfTheDay();
        long currTIme = System.currentTimeMillis();
        long endTime = baseTime + (long)(compactionSchedule.getEndHourOfTheDay() * DateUtils.MILLIS_PER_HOUR);
        if (currTIme < endTime) {
            return false;
        }
        return true;
    }

    public static boolean canStart(CompactionSchedule compactionSchedule) {
        long baseTime = getStartOfTheDay();
        long currTime = System.currentTimeMillis();
        long startTime = baseTime + (long)(compactionSchedule.getStartHourOfTheDay() * DateUtils.MILLIS_PER_HOUR);
        if (currTime >= startTime) {
            return true;
        }
        return false;
    }

}
