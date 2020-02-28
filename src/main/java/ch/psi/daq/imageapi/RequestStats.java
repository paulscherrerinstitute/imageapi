package ch.psi.daq.imageapi;

import org.springframework.http.HttpHeaders;
import org.springframework.web.server.ServerWebExchange;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public final Map<String, Microseconds> channelSeekDurations = new HashMap<>();
    public Instant rangeBegin;
    public Instant rangeEnd;

    RequestStats(ServerWebExchange xc) {
        address = xc.getRequest().getRemoteAddress();
        HttpHeaders headers = xc.getRequest().getHeaders();
        headerAccept = headers.getOrDefault("Accept", List.of());
        headerAcceptEncoding = headers.getOrDefault("Accept-Encoding", List.of());
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

    public void addChannelSeekDuration(String k, Microseconds v) {
        synchronized (channelSeekDurations) {
            channelSeekDurations.put(k, v);
        }
    }

}
