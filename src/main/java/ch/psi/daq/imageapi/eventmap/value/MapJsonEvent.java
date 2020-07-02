package ch.psi.daq.imageapi.eventmap.value;

import com.fasterxml.jackson.databind.JsonNode;

public class MapJsonEvent implements MapJsonItem {

    public long ts;
    public long pulse;
    public JsonNode data;

    @Override
    public void release() {
    }

}
