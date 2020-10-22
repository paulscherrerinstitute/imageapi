package ch.psi.daq.imageapi.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

public class AggMin implements AggFunc {
    double min = Double.POSITIVE_INFINITY;

    public String name() { return "min"; }

    public void sink(JsonNode node) {
        if (node.isNumber()) {
            double v = node.asDouble();
            if (v < min) {
                min = v;
            }
        }
    }

    public void reset() {
    }

    public JsonNode result() {
        if (min == Double.POSITIVE_INFINITY) {
            return JsonNodeFactory.instance.nullNode();
        }
        return JsonNodeFactory.instance.numberNode(min);
    }

}
