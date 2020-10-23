package ch.psi.daq.imageapi.pod.api1;

import java.util.Map;

public class EventAggregated {
    public Ts ts;
    public long pulse;
    public long eventCount;
    public Map<String, AggResult> data;
}
