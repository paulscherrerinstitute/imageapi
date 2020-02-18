package ch.psi.daq.imageapi;

import com.fasterxml.jackson.annotation.JsonGetter;

import java.util.stream.Collector;

class Average {
    public long count;
    public double avg;
    @JsonGetter
    public int avg() { return (int)avg; }
}

class AvgAcc<T extends Number> {
    long count;
    long sum;
    static <T extends Number> void accumulate(AvgAcc<T> a, T x) {
        a.count += 1;
        a.sum += x.longValue();
    }
    static <T extends Number> AvgAcc<T> combine(AvgAcc<T> x, AvgAcc<T> y) {
        x.count += y.count;
        x.sum += y.sum;
        return x;
    }
    static <T extends Number> Average finalize(AvgAcc<T> a) {
        Average ret = new Average();
        ret.count = a.count;
        ret.avg = (double)a.sum / a.count;
        return ret;
    }
}

public class AverageCollector {

    public static <T extends Number> Collector<T, ?, Average> create() {
        return Collector.of(
            AvgAcc<T>::new,
            AvgAcc::accumulate,
            AvgAcc::combine,
            AvgAcc::finalize
        );
    }

}
