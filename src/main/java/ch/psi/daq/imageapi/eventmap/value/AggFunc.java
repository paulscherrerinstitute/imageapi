package ch.psi.daq.imageapi.eventmap.value;

import ch.psi.daq.imageapi.pod.api1.AggResult;
import com.fasterxml.jackson.databind.JsonNode;

public interface AggFunc {
    String name();
    void sink(JsonNode node);
    void reset();
    AggResult result();
}
