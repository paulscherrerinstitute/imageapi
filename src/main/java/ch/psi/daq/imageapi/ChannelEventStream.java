package ch.psi.daq.imageapi;

import ch.psi.daq.imageapi.finder.KeyspaceOrder2;
import ch.psi.daq.imageapi.finder.TimeBin2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public class ChannelEventStream {
    static Logger LOGGER = LoggerFactory.getLogger(ChannelEventStream.class);
    DataBufferFactory bufFac;

    public ChannelEventStream(DataBufferFactory bufFac) {
        this.bufFac = bufFac;
    }

    public static <T> Mono<List<Flux<T>>> dataFluxFromFiles(KeyspaceToDataParams toDataParams, MapFunctionFactory<T> transFac) {
        long beginNanos = toDataParams.begin.toEpochMilli() * 1000000L;
        return Flux.fromIterable(toDataParams.ksp.splits)
        .filter(x -> toDataParams.splits.isEmpty() || toDataParams.splits.contains(x.split))
        .map(sp -> {
            LOGGER.info("{}  open split {}", toDataParams.req.getId(), sp.split);
            sp.timeBins.sort(TimeBin2::compareTo);
            KeyspaceOrder2 ksp = toDataParams.ksp;
            return Flux.fromIterable(sp.timeBins)
            .map(tb -> {
                return Tuples.of(
                    tb,
                    String.format("%s/%s_%s/byTime/%s/%019d/%010d/%019d_%05d_Data", ksp.channel.base.baseDir, ksp.channel.base.baseKeyspaceName, ksp.ksp, ksp.channel.name, tb.timeBin, sp.split, tb.binSize, 0),
                    sp.split
                );
            })
            .map(x -> Tuples.of(x.getT1(), Path.of(x.getT2()), x.getT3()))
            .index()
            .map(x -> {
                int fileno = (int) (long) x.getT1();
                int split = x.getT2().getT3();
                Path path = x.getT2().getT2();
                try {
                    long fsize = Files.size(path);
                    LOGGER.info(String.format("%s  open datafile  %d  %d  %s", toDataParams.req.getId(), x.getT1(), fsize, path));
                    if (x.getT2().getT1().hasIndex) {
                        return Optional.of(PositionedDatafile.openAndPosition(path, beginNanos, fileno, split));
                    }
                    else {
                        return Optional.of(PositionedDatafile.openAndPositionNoIndex(path, beginNanos, fileno, split));
                    }
                }
                catch (IOException e) {
                    LOGGER.warn(String.format("%s  datafile not present  %d  %s", toDataParams.req.getId(), x.getT1(), path));
                    return Optional.<Mono<PositionedDatafile>>empty();
                }
            })
            .doOnNext(x -> {
                LOGGER.info("{}  dataFluxFromFiles  present {}  channel {}  split {}", toDataParams.req.getId(), x.isPresent(), ksp.channel.name, sp.split);
            })
            .filter(Optional::isPresent)
            .concatMap(opt -> {
                LOGGER.info("{}  dataFluxFromFiles prepare openAndPosition Mono before concatMap", toDataParams.req.getId());
                if (opt == null) {
                    throw new RuntimeException("null ptr");
                }
                return opt.get();
            }, 1)
            .concatMap(f -> {
                LOGGER.info("{}  read byte channel with buffersize {}  fileno {}", toDataParams.req.getId(), toDataParams.bufferSize, f.fileno);
                Flux<DataBuffer> fl = DataBufferUtils.readByteChannel(() -> f.takeChannel(), toDataParams.bufFac, toDataParams.bufferSize)
                .doOnDiscard(Object.class, obj -> {
                    LOGGER.error("DISCARD 982u3roiajcsjfo");
                });
                return transFac.makeTrans(fl, toDataParams, f.fileno);
            }, 1);
        })
        .collectList();
    }

}
