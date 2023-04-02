package com.flipkart.yak.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Objects;

@Getter @Setter
@AllArgsConstructor
public class CompactionSchedule {
    @JsonProperty
    @NonNull
    private final float startHourOfTheDay;
    @JsonProperty
    @NonNull
    private final float endHourOfTheDay;


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
