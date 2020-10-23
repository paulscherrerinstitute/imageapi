package ch.psi.daq.imageapi.eventmap.value;

import ch.psi.daq.imageapi.pod.api1.Range;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public class BinFind {
    public ZonedDateTime ts1;
    public ZonedDateTime ts2;
    public long off;
    public long dt;
    public int nBins;

    public BinFind(int nBins, Range range) {
        this.nBins = nBins;
        this.ts1 = ZonedDateTime.parse(range.startDate);
        this.ts2 = ZonedDateTime.parse(range.endDate);
        this.off = 1000000000L * ts1.toInstant().getEpochSecond() + (long) ts1.toInstant().getNano();
        dt = 1000 * ChronoUnit.MICROS.between(ts1, ts2);
    }

    public int find(long ts) {
        if (ts < off) {
            throw new RuntimeException("timestamp smaller than start of range");
        }
        return (int) ((ts - off) / (dt / nBins));
    }

}
