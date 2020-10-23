package ch.psi.daq.imageapi.pod.api1;

import com.fasterxml.jackson.annotation.JsonValue;

public class AggResultSum implements AggResult {
    @JsonValue
    public double sum;
}
