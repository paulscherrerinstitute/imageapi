package ch.psi.daq.imageapi;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

public class ConfigurationRetrieval {

    public static class InvalidException extends RuntimeException {
        public InvalidException(String msg) {
            super(msg);
        }
    }

    public List<SplitNode> splitNodes;
    public boolean mergeLocal;
    public ConfigurationDatabase database;
    public String backend;

    @Override
    public String toString() {
        try {
            return new ObjectMapper(new JsonFactory()).writeValueAsString(this);
        }
        catch (JsonProcessingException e) {
            return String.format("%s", e);
        }
    }

    public void validate() {
        if (backend == null) {
            throw new InvalidException("backend missing");
        }
    }

}
