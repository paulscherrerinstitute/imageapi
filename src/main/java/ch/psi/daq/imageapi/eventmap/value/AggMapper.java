package ch.psi.daq.imageapi.eventmap.value;

import ch.psi.daq.imageapi.pod.api1.EventAggregated;
import ch.psi.daq.imageapi.pod.api1.Ts;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class AggMapper {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger(AggMapper.class);
    JsonGenerator jgen;
    JgenState jst;
    BinFind binFind;
    static long INIT = Long.MIN_VALUE;
    AtomicLong binCurrent = new AtomicLong(INIT);
    AtomicLong binTs = new AtomicLong();
    AtomicLong binPulse = new AtomicLong();
    AtomicLong evCount = new AtomicLong();
    List<AggFunc> aggs;
    OutputBuffer outbuf;
    public AggMapper(JsonGenerator jgen, JgenState jst, BinFind binFind, List<AggFunc> aggs, OutputBuffer outbuf) {
        this.jgen = jgen;
        this.jst = jst;
        this.binFind = binFind;
        this.aggs = aggs;
        this.outbuf = outbuf;
    }
    public List<DataBuffer> map(MapJsonResult res) {
        try {
            for (MapJsonItem item : res.items) {
                if (item instanceof MapJsonChannelStart) {
                    LOGGER.info("Channel Start");
                    MapJsonChannelStart ch = (MapJsonChannelStart) item;
                    jst.beOutOfChannel(jgen);
                    jst.inChannel = true;
                    jgen.writeStartObject();
                    jgen.writeStringField("name", ch.name);
                    jgen.writeFieldName("data");
                    jgen.writeStartArray();
                }
                else if (item instanceof MapJsonEvent) {
                    MapJsonEvent ev = (MapJsonEvent) item;
                    if (binFind != null) {
                        int bin = binFind.find(ev.ts);
                        LOGGER.warn("bin {}  binCurrent {}", bin, binCurrent.get());
                        if (binCurrent.get() == INIT) {
                            binCurrent.set(bin);
                            binTs.set(ev.ts);
                            binPulse.set(ev.pulse);
                        }
                        else if (bin != binCurrent.get()) {
                            EventAggregated eva = new EventAggregated();
                            eva.ts = new Ts(binTs.get());
                            eva.pulse = binPulse.get();
                            eva.eventCount = evCount.get();
                            eva.data = new TreeMap<>();
                            for (AggFunc f : aggs) {
                                eva.data.put(f.name(), f.result());
                            }
                            jgen.writeObject(eva);
                            binCurrent.set(bin);
                            binTs.set(ev.ts);
                            binPulse.set(ev.pulse);
                            evCount.set(0);
                            for (AggFunc f : aggs) {
                                f.reset();
                            }
                        }
                        evCount.getAndAdd(1);
                        for (AggFunc f : aggs) {
                            f.sink(ev.data);
                        }
                    }
                    else {
                        jgen.writeStartObject();
                        jgen.writeFieldName("ts");
                        jgen.writeStartObject();
                        jgen.writeNumberField("sec", ev.ts / 1000000000L);
                        jgen.writeNumberField("ns", ev.ts % 1000000000L);
                        jgen.writeEndObject();
                        jgen.writeNumberField("pulse", ev.pulse);
                        jgen.writeFieldName("data");
                        ev.data.serialize(jgen, new DefaultSerializerProvider.Impl());
                        //jgen.writeObject(ev.data);
                        jgen.writeEndObject();
                    }
                }
                else {
                    throw new RuntimeException("logic");
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        res.release();
        return outbuf.getPending();
    }
    public Mono<List<DataBuffer>> finalResult() {
        try {
            if (jst.inChannel) {
                if (binFind != null) {
                    EventAggregated eva = new EventAggregated();
                    eva.ts = new Ts(binTs.get());
                    eva.pulse = binPulse.get();
                    eva.eventCount = evCount.get();
                    eva.data = new TreeMap<>();
                    for (AggFunc f : aggs) {
                        eva.data.put(f.name(), f.result());
                    }
                    jgen.writeObject(eva);
                }
            }
            jst.beOutOfChannel(jgen);
            jgen.writeEndArray();
            jgen.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Mono.just(outbuf.getPending());
    }
}
