package ch.psi.daq.imageapi;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

public class ConfigurationRetrieval {
    public List<SplitNode> splitNodes;
    public boolean mergeLocal;
    public ConfigurationDatabase database;

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
