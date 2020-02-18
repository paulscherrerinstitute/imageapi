package ch.psi.daq.imageapi3;

import org.springframework.http.HttpHeaders;
import org.springframework.web.server.ServerWebExchange;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RequestStats {
    public InetSocketAddress address;
    public List<String> headerAccept;
    public List<String> headerAcceptEncoding;
    public Microseconds getChannelNamesDuration;
    public Microseconds checkAllChannelsContained;
    public Microseconds locateDataFiles;
    public Microseconds totalDuration;
    long requestTimeBegin = System.nanoTime();
    public final Map<String, ChannelIOStats> channelIOStats = new HashMap<>();
    public Instant rangeBegin;
    public Instant rangeEnd;

    RequestStats(ServerWebExchange xc) {
        address = xc.getRequest().getRemoteAddress();
        HttpHeaders headers = xc.getRequest().getHeaders();
        headerAccept = headers.getOrDefault("Accept", List.of());
        headerAccept = headers.getOrDefault("Accept-Encoding", List.of());
    }

    public static RequestStats empty(ServerWebExchange xc) {
        return new RequestStats(xc);
    }

    public void endNow() {
        totalDuration = Microseconds.fromNanos(System.nanoTime() - requestTimeBegin);
    }

    public void addChannelIOStats(String k, ChannelIOStats v) {
        synchronized (channelIOStats) {
            channelIOStats.put(k, v);
        }
    }

}
