package ch.psi.daq.imageapi.merger;

import ch.qos.logback.classic.Logger;
import org.reactivestreams.Subscription;
import org.slf4j.LoggerFactory;

public class MergerSubscription implements Subscription {
    Logger LOGGER = (Logger) LoggerFactory.getLogger("MergerSubscription");
    Merger merger;
    int inReq;

    MergerSubscription(Merger merger) {
        this.merger = merger;
    }

    @Override
    public void request(long n) {
        inReq += 1;
        merger.request(n, inReq);
        inReq -= 1;
    }

    @Override
    public void cancel() {
        merger.cancel();
    }

}
