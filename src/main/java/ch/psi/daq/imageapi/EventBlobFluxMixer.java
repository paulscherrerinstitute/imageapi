package ch.psi.daq.imageapi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.Comparator;
import java.util.List;

public class EventBlobFluxMixer {
    static Logger LOGGER = LoggerFactory.getLogger(EventBlobFluxMixer.class);
    List<PositionedDataBufferFlux> fluxes;
    final long beginNano;
    final long endNano;
    final int FETCH = 2;
    RequestStats reqSt;

    EventBlobFluxMixer(List<PositionedDataBufferFlux> fluxes, long beginNano, long endNano, RequestStats reqSt) {
        this.fluxes = fluxes;
        this.beginNano = beginNano;
        this.endNano = endNano;
        this.reqSt = reqSt;
    }

    public static EventBlobFluxMixer create(List<PositionedDataBufferFlux> fluxes, long beginNano, long endNano, RequestStats reqSt) {
        return new EventBlobFluxMixer(fluxes, beginNano, endNano, reqSt);
    }

    public Flux<EventDataVectored> eventData() {
        return eventDataMerged()
        .takeWhile(x -> x.ts < endNano && x.ts >= beginNano)
        .doOnDiscard(EventDataVectored.class, x -> {
            LOGGER.info("doOnDiscard");
            x.release();
        })
        .doOnComplete(() -> {
            //LOGGER.info("merged events flux complete");
            fluxes.forEach(x -> x.doOnComplete());
        });
    }

    public Flux<EventDataVectored> eventDataMerged() {
        if (fluxes.size() <= 0) {
            return Flux.empty();
        }
        if (fluxes.size() == 1) {
            return fluxes.get(0).eventDataFlux();
        }
        Flux<EventDataVectored> ret = fluxes.get(0).eventDataFlux();
        for (int i = 1; i < fluxes.size(); i++) {
            ret = Flux.mergeOrdered(FETCH, Comparator.comparingLong(a -> a.ts), ret, fluxes.get(i).eventDataFlux());
        }
        return ret;
    }

}
