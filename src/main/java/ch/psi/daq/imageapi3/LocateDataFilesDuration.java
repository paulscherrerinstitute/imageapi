package ch.psi.daq.imageapi3;

import com.fasterxml.jackson.annotation.JsonGetter;

public class LocateDataFilesDuration {
    public String channelName;
    Microseconds duration;
    public static LocateDataFilesDuration create(String channelName, Microseconds duration) {
        LocateDataFilesDuration x = new LocateDataFilesDuration();
        x.channelName = channelName;
        x.duration = duration;
        return x;
    }
    @JsonGetter("durationMus")
    public long durationMus() { return duration.mu(); }
}
