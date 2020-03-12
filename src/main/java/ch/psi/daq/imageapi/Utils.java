package ch.psi.daq.imageapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Utils {
    static Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    static class SeekError extends Exception {
        static SeekError empty() {
            return new SeekError();
        }
    }

    public static class MatchingDataFilesResult {
        // list[timebin][nodebin aka 'split']
        List<List<Path>> list;
        Microseconds duration;
    }

    public static Mono<MatchingDataFilesResult> matchingDataFiles(IFileManager fileManager, String channelName, Instant start, Instant end) {
        return Mono.fromCallable(() -> {
            long tt1 = System.nanoTime();
            // returns List<Path> [bin][split]
            List<List<Path>> l = fileManager.locateDataFiles(channelName, start, end);
            long tt2 = System.nanoTime();
            MatchingDataFilesResult ret = new MatchingDataFilesResult();
            ret.list = l;
            ret.duration = Microseconds.fromNanos(tt2 - tt1);
            return ret;
        })
        .subscribeOn(Schedulers.boundedElastic());
    }

    /*
    Seeking is too slow. Currently not used, but maybe needed as backup solution.
    */
    public static Mono<PositionedDatafile> openAndPositionWithoutIndex(Path path, String channelName, Instant begin) {
        return Mono.fromCallable(() -> {
            LOGGER.info("openAndPositionWithoutIndex on thread {}", Thread.currentThread().getName());
            SeekableByteChannel c = Files.newByteChannel(path, StandardOpenOption.READ);
            SeekToBeginResult res = Utils.seekToBegin(c, begin);
            if (!res.isOk()) { throw new RuntimeException("Could not seek.  TODO handle this more gracefully."); }
            LOGGER.debug("lengthOfFirstBlob: {}", res.lengthOfFirstBlob());
            return PositionedDatafile.fromChannel(c, path);
        })
        .subscribeOn(Schedulers.boundedElastic());
    }

    static class SeekToBeginResult {
        long lengthOfFirstBlob;
        static SeekToBeginResult ok(long lengthOfFirstBlob) {
            SeekToBeginResult ret = new SeekToBeginResult();
            ret.lengthOfFirstBlob = lengthOfFirstBlob;
            return ret;
        }
        static SeekToBeginResult error() {
            SeekToBeginResult ret = new SeekToBeginResult();
            ret.lengthOfFirstBlob = -1;
            return ret;
        }
        public boolean isOk() { return lengthOfFirstBlob >= 0; }
        public long lengthOfFirstBlob() { return lengthOfFirstBlob; }
    }

    static SeekToBeginResult seekToBegin(SeekableByteChannel ch, Instant begin) {
        LOGGER.info("seekToBegin on thread {}", Thread.currentThread().getName());
        long tgtBeginNano = instantToNanoLong(begin);
        LOGGER.info("search for tgtBeginNano: {}", tgtBeginNano);
        long n = -1;
        try {
            ByteBuffer buf = Utils.allocateByteBuffer(1<<22);
            buf.order(ByteOrder.BIG_ENDIAN);
            buf.rewind();
            buf.limit(1<<12);
            n = ch.read(buf);
            // TODO..
            if (n != buf.limit()) {
                LOGGER.info("small file?  {}", n);
            }
            if (n < Short.BYTES + Integer.BYTES) {
                throw new IOException("not read enough");
            }
            LOGGER.info("buf pos: {}  lim: {}", buf.position(), buf.limit());
            buf.flip();
            buf.getShort();
            // What if the file contains no event blob at all?
            int lengthChannelNamePlusTwoInts = buf.getInt();
            if (lengthChannelNamePlusTwoInts < 2 * Integer.BYTES || lengthChannelNamePlusTwoInts > 1024) {
                throw new RuntimeException(String.format("something is fishy with lengthChannelNamePlusTwoInts: %d", lengthChannelNamePlusTwoInts));
            }
            int lengthChannelName = lengthChannelNamePlusTwoInts - 2 * Integer.BYTES;
            String channelName = StandardCharsets.UTF_8.decode(buf.slice().limit(lengthChannelName)).toString();
            LOGGER.info("channelName: {}", channelName);
            buf.position(buf.position() + lengthChannelName);
            int lengthChannelName2 = buf.getInt() - 2 * Integer.BYTES;
            if (lengthChannelName != lengthChannelName2) {
                throw new RuntimeException(String.format("something is fishy with lengthChannelName2: %d  vs  %d", lengthChannelName, lengthChannelName2));
            }
            long currentPos = buf.position();
            ch.position(currentPos);
            while (true) {
                TryNextBlobAtResult res = Utils.tryNextBlobAt(ch, currentPos, buf);
                if (!res.avail) {
                    LOGGER.error("Unable to seek to position");
                    // TODO log this
                    ch.position(ch.size());
                    return SeekToBeginResult.error();
                }
                else {
                    if (res.ts >= tgtBeginNano) {
                        LOGGER.info("Found BEGIN at abspos {}", currentPos);
                        ch.position(currentPos);
                        return SeekToBeginResult.ok(res.absPosAfterThis - currentPos);
                    }
                    else {
                        if (res.absPosAfterThis <= currentPos) {
                            LOGGER.error("something fishy in file {}", channelName);
                            ch.position(ch.size());
                            return SeekToBeginResult.error();
                        }
                        currentPos = res.absPosAfterThis;
                        ch.position(currentPos);
                    }
                }
            }
        }
        catch (IOException e) {
            LOGGER.error("IOException: {}", e.toString());
            return SeekToBeginResult.error();
        }
    }

    static class TryNextBlobAtResult {
        boolean avail;
        long ts;
        long absPosAfterThis;
    }

    static TryNextBlobAtResult tryNextBlobAt(SeekableByteChannel ch, long currentPos, ByteBuffer buf) throws IOException {
        buf.clear();
        buf.limit(1<<11);
        ch.read(buf);
        buf.flip();
        if (buf.remaining() >= Integer.BYTES + 2 * Long.BYTES) {
            int lengthBlob = buf.getInt();
            long ttl = buf.getLong();
            long ts = buf.getLong();
            long pulseId = buf.getLong();
            LOGGER.info("lengthBlob: {}  ts: {}", lengthBlob, ts);
            long seekTo = currentPos + lengthBlob;
            TryNextBlobAtResult ret = new TryNextBlobAtResult();
            ret.avail = true;
            ret.ts = ts;
            ret.absPosAfterThis = seekTo;
            return ret;
        }
        else {
            return new TryNextBlobAtResult();
        }
    }

    // TODO
    static long instantToNanoLong(Instant begin) { return 1000000 * begin.toEpochMilli(); }

    static class BlobJsonHeader {
        public String name;
        public String type;
        public String compression;
        public String byteOrder;
        public List<Integer> shape;
    }

    public static ByteBuffer allocateByteBuffer(int len) {
        return ByteBuffer.allocate(len);
    }

}
