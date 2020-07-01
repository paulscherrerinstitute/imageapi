package ch.psi.daq.imageapi;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

public class RetrievalConfiguration {
    public List<SplitNode> splitNodes;

    @Override
    public String toString() {
        try {
            return new ObjectMapper(new JsonFactory()).writeValueAsString(this);
        }
        catch (JsonProcessingException e) {
            return String.format("%s", e);
        }
    }
}
