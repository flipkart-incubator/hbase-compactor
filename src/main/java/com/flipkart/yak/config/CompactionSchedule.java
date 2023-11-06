package com.flipkart.yak.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.util.Objects;

@Data
@SuperBuilder
@RequiredArgsConstructor
@Jacksonized
public class CompactionSchedule {
    final float startHourOfTheDay;

    final float endHourOfTheDay;

    boolean isPrompt = false;

    CompactionScheduleLifeCycle compactionScheduleLifeCycle;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompactionSchedule)) return false;
        CompactionSchedule that = (CompactionSchedule) o;
        return getStartHourOfTheDay() == that.getStartHourOfTheDay() &&
                getEndHourOfTheDay() == that.getEndHourOfTheDay();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getStartHourOfTheDay(), getEndHourOfTheDay());
    }


}
