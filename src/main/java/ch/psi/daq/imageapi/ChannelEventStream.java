package ch.psi.daq.imageapi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class ChannelEventStream {
    static Logger LOGGER = LoggerFactory.getLogger(ChannelEventStream.class);
    DataBufferFactory bufFac;
    static Throttle throttle = new Throttle();

    ChannelEventStream(DataBufferFactory bufFac) {
        this.bufFac = bufFac;
    }

    class PositionedChannelsList {
        List<PositionedDatafile> channels;
        Microseconds duration;
    }

    static void touchBuffer(DataBuffer buf) {
        long y = buf.getByte(0);
        if (y * y == Long.MAX_VALUE - 7) {
            LOGGER.error("hit max");
        }
        buf.write(new byte[] {0});
        buf.writePosition(buf.writePosition() - 1);
    }

    public Flux<DataBuffer> bufferFluxFromFiles(ChannelWithFiles channelWithFiles, RequestStats requestStats, long beginNano, long endNano) {
        requestStats.locateDataFiles = channelWithFiles.files.duration;
        List<List<Path>> listOverBins = channelWithFiles.files.list;
        EventDataVectoredPostProcess pp = new EventDataVectoredPostProcess(channelWithFiles, requestStats);
        return Flux.fromIterable(listOverBins)
        .flatMapSequential(listOverSplits -> {
            final long t1 = System.nanoTime();
            return Flux.fromIterable(listOverSplits)
            .flatMapSequential(path -> PositionedDatafile.openAndPosition(path, channelWithFiles.name, beginNano), 1)
            .collectList()
            .flatMap(x -> Mono.just(x).zipWith(Mono.just(Microseconds.fromNanos(System.nanoTime() - t1))));
        }, 1)
        .map(x -> {
            PositionedChannelsList r = new PositionedChannelsList();
            r.channels = x.getT1();
            r.duration = x.getT2();
            return r;
        })
        .doOnNext(x -> {
            requestStats.addChannelSeekDuration(channelWithFiles.name, x.duration);
        })
        .flatMapSequential(pcl ->
        Flux.fromIterable(pcl.channels).index()
        .map(channelIndexed -> {
            long pos = -1;
            try {
                pos = channelIndexed.getT2().channel.position();
            }
            catch (IOException e) {
                LOGGER.error("can not get position after seek for channel");
                throw new RuntimeException("IOException while query position");
            }
            return PositionedDataBufferFlux.create(
            channelIndexed.getT2().channel,
            DataBufferUtils.readByteChannel(() -> channelIndexed.getT2().channel, bufFac, 1 * 1024 * 1024),
            channelIndexed.getT1().intValue(),
            pos
            );
        })
        .collectList(),
        1
        )
        .flatMapSequential(x -> EventBlobFluxMixer.create(x, beginNano, endNano, requestStats).eventData(), 1)
        .doOnNext(pp::postProcess)
        .flatMapSequential(x -> Flux.fromIterable(x.blobs), 1)
        .flatMapSequential(throttle::map, 1);
    }

    class Pulse {
        public long ts;
        public long pulse;
        public long blobLength;
    }

    public Flux<Pulse> listPulses(ChannelWithFiles channelWithFiles, RequestStats requestStats, long beginNano, long endNano) {
        requestStats.locateDataFiles = channelWithFiles.files.duration;
        List<List<Path>> listOverBins = channelWithFiles.files.list;
        return Flux.fromIterable(listOverBins)
        .flatMapSequential(listOverSplits -> {
            final long t1 = System.nanoTime();
            return Flux.fromIterable(listOverSplits)
            .flatMapSequential(path -> PositionedDatafile.openAndPosition(path, channelWithFiles.name, beginNano), 1)
            .collectList()
            .flatMap(x -> Mono.just(x).zipWith(Mono.just(Microseconds.fromNanos(System.nanoTime() - t1))));
        }, 1)
        .map(x -> {
            PositionedChannelsList r = new PositionedChannelsList();
            r.channels = x.getT1();
            r.duration = x.getT2();
            return r;
        })
        .doOnNext(x -> {
            requestStats.addChannelSeekDuration(channelWithFiles.name, x.duration);
        })
        .flatMapSequential(pcl ->
            Flux.fromIterable(pcl.channels).index()
            .map(channelIndexed -> {
                long pos = -1;
                try {
                    pos = channelIndexed.getT2().channel.position();
                }
                catch (IOException e) {
                    LOGGER.error("can not get position after seek for channel");
                    throw new RuntimeException("IOException while query position");
                }
                return PositionedDataBufferFlux.create(
                    channelIndexed.getT2().channel,
                    DataBufferUtils.readByteChannel(() -> channelIndexed.getT2().channel, bufFac, 10 * 1024 * 1024),
                    channelIndexed.getT1().intValue(),
                    pos
                );
            })
            .collectList(),
            1
        )

        .flatMapSequential(x -> EventBlobFluxMixer.create(x, beginNano, endNano, requestStats).eventData(), 1)
        .map(x -> {
            Pulse ret = new Pulse();
            ret.ts = x.ts;
            ret.pulse = x.pulse;
            ret.blobLength = x.totalValueLength();
            return ret;
        });
    }

}
