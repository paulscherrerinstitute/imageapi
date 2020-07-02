package ch.psi.daq.imageapi;

import ch.psi.daq.imageapi.finder.BaseDir;
import ch.psi.daq.imageapi.finder.BaseDirFinderFormatV0;
import ch.psi.daq.imageapi.finder.KeyspaceOrder2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

public class PositionedDatafile {
    static Logger LOGGER = LoggerFactory.getLogger(PositionedDatafile.class);

    public SeekableByteChannel channel;
    public Path path;
    public int fileno;

    PositionedDatafile(SeekableByteChannel channel, Path path, int fileno) {
        this.path = path;
        this.channel = channel;
        this.fileno = fileno;
    }

    /**
     * @param channel The byte channel.
     * @param path For error handling, remember the path of the byte channel.
     */
    public static PositionedDatafile fromChannel(SeekableByteChannel channel, Path path, int fileno) {
        return new PositionedDatafile(channel, path, fileno);
    }

    public static Mono<PositionedDatafile> openAndPosition(Path path, long beginNano) {
        return openAndPosition(path, beginNano, -1, 0xcafe);
    }

    public static Mono<PositionedDatafile> openAndPosition(Path path, long beginNano, int fileno, int split) {
        return Index.openIndex(Path.of(path.toString() + "_Index"))
        .map(x -> Index.findGEByLong(beginNano, x))
        .flatMap(x -> {
            if (!x.isSome()) {
                return Mono.fromCallable(() -> {
                    LOGGER.info("Seek  fileno {}  split {}   NONE FOUND SEEK TO END", fileno, split);
                    SeekableByteChannel c = Files.newByteChannel(path, StandardOpenOption.READ);
                    // TODO verify correct channel name here
                    c.position(c.size());
                    return PositionedDatafile.fromChannel(c, path, fileno);
                });
            }
            return Mono.fromCallable(() -> {
                LOGGER.info("Seek  fileno {}  split {}  position {}", fileno, split, x.v);
                SeekableByteChannel c = Files.newByteChannel(path, StandardOpenOption.READ);
                // TODO verify correct channel name here
                if (x.v >= c.size()) {
                    throw new RuntimeException("Seek error");
                }
                c.position(x.v);
                return PositionedDatafile.fromChannel(c, path, fileno);
            });
        })
        .subscribeOn(Schedulers.parallel());
    }

    public static Mono<PositionedDatafile> openAndPositionNoIndex(Path path, long beginNano) {
        return openAndPositionNoIndex(path, beginNano, -1, 0xcafe);
    }

    public static Mono<PositionedDatafile> openAndPositionNoIndex(Path path, long beginNano, int fileno, int split) {
        LOGGER.info("openAndPositionNoIndex  beginNano {}  fileno {}  split {}  path {}", beginNano, fileno, split, path);
        return Mono.fromCallable(() -> {
            long fileSize = Files.size(path);
            SeekableByteChannel chn = Files.newByteChannel(path, StandardOpenOption.READ);
            ByteBuffer buf = ByteBuffer.allocate(2048);
            chn.read(buf);
            buf.flip();
            if (buf.remaining() < 6) {
                throw new RuntimeException("bad datafile");
            }
            buf.getShort();
            int j2 = buf.getInt();
            if (j2 < 1 || j2 > 256) {
                throw new RuntimeException("bad channelname in datafile");
            }
            long posBegin = 2 + j2;
            buf.position((int)posBegin);
            int blobLen = buf.getInt();
            if (blobLen < 0 || blobLen > 1024) {
                throw new RuntimeException("bad blobLen");
            }
            if (fileSize < posBegin + blobLen) {
                throw new RuntimeException("not a single blob in file");
            }
            buf.getLong();
            long tsMin = buf.getLong();
            if (tsMin >= beginNano) {
                chn.position(posBegin);
                return PositionedDatafile.fromChannel(chn, path, fileno);
            }
            long posEnd = ((fileSize - posBegin - 1) / blobLen) * blobLen + posBegin;
            if (posEnd < posBegin) {
                throw new RuntimeException("bad file structure");
            }
            //throw new RuntimeException(String.format("blob length is: %d  tsMin: %d  posBegin: %d  posEnd: %d", blobLen, tsMin, posBegin, posEnd));
            buf.clear();
            buf.limit(blobLen);
            chn.position(posEnd);
            chn.read(buf);
            buf.flip();
            if (buf.remaining() != blobLen) {
                throw new RuntimeException("can not read a full blob");
            }
            if (buf.getInt(0) != blobLen) {
                throw new RuntimeException("invalid blob len encountered");
            }
            long tsMax = buf.getLong(12);
            if (tsMax < beginNano) {
                chn.position(posEnd);
                return PositionedDatafile.fromChannel(chn, path, fileno);
            }
            long j = posBegin;
            long k = posEnd;
            while (k - j >= 2 * blobLen) {
                long m = j + (k-j) / blobLen / 2 * blobLen;
                chn.position(m);
                buf.clear();
                buf.limit(blobLen);
                chn.read(buf);
                buf.flip();
                if (buf.remaining() != blobLen) {
                    throw new RuntimeException("can not read a full blob");
                }
                long ts = buf.getLong(12);
                if (ts >= beginNano) {
                    k = m;
                }
                else {
                    j = m;
                }
            }
            chn.position(k);
            return PositionedDatafile.fromChannel(chn, path, fileno);
            //throw new RuntimeException(String.format("blob length is: %d  tsMin: %d  tsMax: %d  posBegin: %d  posEnd: %d", blobLen, tsMin, tsMax, posBegin, posEnd));
            //return PositionedDatafile.fromChannel(chn, path);
        });
    }

    public static Mono<List<Flux<PositionedDatafile>>> positionedDatafilesFromKeyspace(KeyspaceOrder2 ksp, Instant begin, Instant end, int bufferSize, List<Integer> splits) {
        long beginNanos = begin.toEpochMilli() * 1000000L;
        long endNanos = end.toEpochMilli() * 1000000L;
        Mono<List<Flux<PositionedDatafile>>> ret = Flux.fromIterable(ksp.splits)
        .filter(x -> splits.isEmpty() || splits.contains(x.split))
        .map(sp -> {
            BaseDir base = ksp.channel.base;
            // TODO need to sort by true start?
            return Flux.fromIterable(sp.timeBins)
            .map(tb -> {
                return Tuples.of(
                tb,
                String.format("%s/%s_%s/byTime/%s/%019d/%010d/%019d_%05d_Data", base.baseDir, base.baseKeyspaceName, ksp.ksp, ksp.channel.name, tb.timeBin, sp.split, tb.binSize, 0)
                );
            })
            .map(x -> Tuples.of(x.getT1(), Path.of(x.getT2())))
            .index()
            .concatMap(x -> {
                long fileId = x.getT1();
                Path path = x.getT2().getT2();
                if (x.getT2().getT1().hasIndex) {
                    return PositionedDatafile.openAndPosition(path, beginNanos);
                }
                else {
                    return PositionedDatafile.openAndPositionNoIndex(path, beginNanos);
                }
            });
        })
        .collectList();
        return ret;
    }

    public SeekableByteChannel takeChannel() {
        SeekableByteChannel c = channel;
        channel = null;
        return c;
    }

    public void release() {
        if (channel != null) {
            try {
                channel.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            channel = null;
        }
    }

}
