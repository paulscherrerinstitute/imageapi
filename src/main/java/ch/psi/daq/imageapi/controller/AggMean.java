package ch.psi.daq.imageapi.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

public class AggMean implements AggFunc {
    double sum = 0.0;
    long n = 0;

    public String name() { return "mean"; }

    public void sink(JsonNode node) {
        if (node.isNumber()) {
            double v = node.asDouble();
            sum += v;
            n += 1;
        }
    }

    public void reset() {
        sum = 0.0;
        n = 0;
    }

    public JsonNode result() {
        return JsonNodeFactory.instance.numberNode(sum / n);
    }

}
