package com.flipkart.yak.commons;

import com.flipkart.yak.config.CompactionSchedule;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

public class SchedulerUtils {

    @ParameterizedTest
    @CsvSource({"2022-11-05T12:30:00Z,0","2022-11-05T12:00:00Z,1800000","2022-11-05T13:00:00Z,84600000"})
    public void testSleepTime(String dateTime, String expectedSleepTime) {
        CompactionSchedule compactionSchedule = new CompactionSchedule(18,20);
        Instant instant = Instant.now(Clock.fixed(Instant.parse( dateTime), ZoneOffset.UTC));
        long sleepTime = ScheduleUtils.getSleepTime(compactionSchedule, instant);
        assert sleepTime == Long.parseLong(expectedSleepTime);
    }

    @ParameterizedTest
    @CsvSource({"2022-11-05T12:30:00Z,false","2022-11-05T12:00:00Z,false","2022-11-05T16:00:00Z,true"})
    public void testScheduleTimeout(String dateTime, String expectedAns) {
        CompactionSchedule compactionSchedule = new CompactionSchedule(18,20);
        Instant instant = Instant.now(Clock.fixed(Instant.parse( dateTime), ZoneOffset.UTC));
        boolean whatToDo = ScheduleUtils.hasTimedOut(compactionSchedule, instant);
        assert whatToDo == Boolean.parseBoolean(expectedAns);
    }

    @ParameterizedTest
    @CsvSource({"2022-11-05T11:30:00Z,false","2022-11-05T12:00:00Z,false","2022-11-05T16:00:00Z,true"})
    public void testScheduleStartEligibility(String dateTime, String expectedAns) {
        CompactionSchedule compactionSchedule = new CompactionSchedule(18,20);
        Instant instant = Instant.now(Clock.fixed(Instant.parse( dateTime), ZoneOffset.UTC));
        boolean whatToDo = ScheduleUtils.canStart(compactionSchedule, instant);
        assert whatToDo == Boolean.parseBoolean(expectedAns);
    }
}
