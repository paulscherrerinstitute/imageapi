package ch.psi.daq.imageapi.controller;

import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.slf4j.LoggerFactory;

public class AggSum implements AggFunc {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger(AggSum.class);

    double sum = 0.0;

    public String name() { return "sum"; }

    public void sink(JsonNode node) {
        //LOGGER.warn("sinking value {}", node);
        if (node.isNumber()) {
            //LOGGER.warn("sinking number {}", node.asDouble());
            double v = node.asDouble();
            sum += v;
        }
    }

    public void reset() {
        sum = 0.0;
    }

    public JsonNode result() {
        return JsonNodeFactory.instance.numberNode(sum);
    }

}
