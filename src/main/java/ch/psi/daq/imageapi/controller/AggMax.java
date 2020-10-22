package ch.psi.daq.imageapi.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

public class AggMax implements AggFunc {
    double max = Double.NEGATIVE_INFINITY;

    public String name() { return "max"; }

    public void sink(JsonNode node) {
        if (node.isNumber()) {
            double v = node.asDouble();
            if (v > max) {
                max = v;
            }
        }
    }

    public void reset() {
    }

    public JsonNode result() {
        if (max == Double.NEGATIVE_INFINITY) {
            return JsonNodeFactory.instance.nullNode();
        }
        return JsonNodeFactory.instance.numberNode(max);
    }

}
