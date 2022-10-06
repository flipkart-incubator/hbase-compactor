package com.flipkart.yak.config;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter @Setter
@AllArgsConstructor
public class CompactionSchedule {
    private final int startHourOfTheDay;
    private final int endHourOfTheDay;

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
