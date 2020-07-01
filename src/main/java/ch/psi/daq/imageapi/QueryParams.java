package ch.psi.daq.imageapi;

import ch.psi.daq.imageapi.pod.api1.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

public class QueryParams {
    static Logger LOGGER = LoggerFactory.getLogger(QueryParams.class);
    public List<String> channels;
    public Instant begin;
    public Instant end;
    public List<Integer> splits;
    public int bufferSize;
    public boolean decompressOnServer;
    public long limitBytes;
    public static QueryParams fromQuery(Query x) {
        QueryParams ret = new QueryParams();
        ret.channels = x.channels;
        ret.begin = Instant.parse(x.range.startDate);
        ret.end = Instant.parse(x.range.endDate);
        ret.bufferSize = x.bufferSize > 0 ? x.bufferSize : 128 * 1024;
        ret.splits = x.splits == null ? List.of() : x.splits;
        ret.decompressOnServer = x.decompressOnServer == 1;
        ret.limitBytes = x.limitBytes;
        if (ret.begin.isAfter(ret.end)) {
            throw new IllegalArgumentException(String.format("Begin date %s is after end date %s", ret.begin, ret.end));
        }
        return ret;
    }
}
