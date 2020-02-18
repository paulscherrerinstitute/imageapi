package ch.psi.daq.imageapi;

import com.fasterxml.jackson.annotation.JsonGetter;

import java.util.stream.Collector;

class Deviation {
    public double dev;
    @JsonGetter
    public int dev() { return (int)dev; }
}

class DevAcc<T extends Number> {
    double sum;
    double avg;
    Average avgb;
    DevAcc(Average avg) {
        this.avg = avg.avg;
        avgb = avg;
    }
    static <T extends Number> void accumulate(DevAcc<T> a, T x) {
        double y = x.doubleValue() - a.avg;
        a.sum += y * y;
    }
    static <T extends Number> DevAcc<T> combine(DevAcc<T> x, DevAcc<T> y) {
        x.sum += y.sum;
        return x;
    }
    static <T extends Number> Deviation finalize(DevAcc<T> a) {
        Deviation ret = new Deviation();
        if (a.avgb.count > 1) {
            ret.dev = Math.sqrt(a.sum / (a.avgb.count-1));
            return ret;
        }
        else {
            return ret;
        }
    }
}

public class DeviationCollector {

    public static <T extends Number> Collector<T, ?, Deviation> create(Average avg) {
        return Collector.of(
        () -> new DevAcc<>(avg),
            DevAcc::accumulate,
            DevAcc::combine,
            DevAcc::finalize
        );
    }

}
