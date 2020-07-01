package ch.psi.daq.imageapi;

import ch.psi.daq.imageapi.finder.KeyspaceOrder2;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.http.server.reactive.ServerHttpRequest;

import java.time.Instant;
import java.util.List;

public class KeyspaceToDataParams {
    public KeyspaceOrder2 ksp;
    public Instant begin;
    public Instant end;
    public DataBufferFactory bufFac;
    public int bufferSize;
    public List<Integer> splits;
    public ServerHttpRequest req;
    public KeyspaceToDataParams(KeyspaceOrder2 ksp, Instant begin, Instant end, DataBufferFactory bufFac, int bufferSize, List<Integer> splits, ServerHttpRequest req) {
        this.ksp = ksp;
        this.begin = begin;
        this.end = end;
        this.bufFac = bufFac;
        this.bufferSize = bufferSize;
        this.splits = splits;
        this.req = req;
    }
}
