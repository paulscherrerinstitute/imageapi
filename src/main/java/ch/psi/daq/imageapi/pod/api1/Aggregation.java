package ch.psi.daq.imageapi.pod.api1;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Aggregation {
    // A better name would be `functions` but need to keep compatibility for now:
    public List<String> aggregations;
    public int nrOfBins;
}
