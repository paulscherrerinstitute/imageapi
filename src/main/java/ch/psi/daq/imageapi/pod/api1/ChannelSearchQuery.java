package ch.psi.daq.imageapi.pod.api1;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ChannelSearchQuery {
    //public List<String> backends;
    public String regex;
    public String ordering;

    public boolean valid() {
        return ordering == null || ordering.equalsIgnoreCase("asc") || ordering.equalsIgnoreCase("desc");
    }

    public Order order() {
        if (ordering != null && ordering.equalsIgnoreCase("desc")) {
            return Order.DESC;
        }
        else {
            return Order.ASC;
        }
    }

}
