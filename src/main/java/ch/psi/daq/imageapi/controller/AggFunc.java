package ch.psi.daq.imageapi.controller;

import com.fasterxml.jackson.databind.JsonNode;

public interface AggFunc {

    String name();
    void sink(JsonNode node);
    void reset();
    JsonNode result();

}
