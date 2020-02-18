package ch.psi.daq.imageapi3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
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
import java.util.Optional;

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

    // TODO why no need to declare SeekError?
    public static Mono<PositionedDatafile> openAndPosition(Path path, String channelName, Instant begin) {
        return Index.openIndex(Path.of(path.toString() + "_Index"))
        .map(x -> Index.findGEByLong(instantToNanoLong(begin), x))
        .flatMap(x -> {
            return Mono.fromCallable(() -> {
                SeekableByteChannel c = Files.newByteChannel(path, StandardOpenOption.READ);
                // TODO verify correct channel name here
                if (x.v >= c.size()) {
                    throw SeekError.empty();
                }
                c.position(x.v);
                return PositionedDatafile.fromChannel(c, path);
            });
        })
        .subscribeOn(Schedulers.boundedElastic());
    }

    static void comparePositions(List<PositionedDatafile> l1, List<PositionedDatafile> l2) {
        if (l1.size() != l2.size()) {
            throw new RuntimeException("Both methods should yield the same");
        }
        for (int i = 0; i < l1.size(); i+=1) {
            try {
                if (l1.get(i).channel.position() != l2.get(i).channel.position()) {
                    throw new RuntimeException("BAD");
                }
            }
            catch (IOException e) {
                throw new RuntimeException("Seeked to different positions");
            }
        }
    }

    // TODO
    static long instantToNanoLong(Instant begin) { return 1000000 * begin.toEpochMilli(); }

    static class BlobJsonHeader {
        public String name;
        public String type;
        public String compression;
        public String byteOrder;
        public Collection<Integer> shape;
    }

    static Flux<ByteBuffer> blobToRestChunk(String channelName, PositionedDatafile.Blob blob, boolean isFirst) {
        ByteBuffer bufHeader = Utils.allocateByteBuffer(2048);
        // getDType expects a buffer that starts with the event TTL
        // and ends with?
        DType dtype = DataBlobUtils.getDType(blob.blobBuf.slice());
        int valueOffset = dtype.getBufferReadOffsetForValue();
        int valueLength = (blob.blobBuf.slice().limit() - valueOffset - 1 * Integer.BYTES);
        //LOGGER.info("valueOffset: {}  valueLength: {}", valueOffset, valueLength);
        if (isFirst) {
            //LOGGER.info("++++++++++++    PUTTING JSON");
            Collection<Integer> shape = new ArrayList<>();
            for (int x : dtype.getShape()) {
                shape.add(x);
            }
            BlobJsonHeader header = new BlobJsonHeader();
            header.name = channelName;
            header.type = dtype.getType().toString().toLowerCase();
            header.compression = String.format("%d", dtype.getCompression().getId());
            header.byteOrder = dtype.getByteOrder().toString();
            header.shape = shape;
            ObjectWriter ow = new ObjectMapper().writer();
            String headerString;
            try {
                headerString = ow.writeValueAsString(header);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("JsonProcessingException");
            }
            /*
            [int length1 of only the bytes between these two length fields]
              [byte(0)]
              [json utf8 string without null terminator]
            [int length1]
            [int length2 of only the bytes between these two length fields]
              [byte(1)]
              [long timestamp]
              [long pulseid]
              [value]
            [int length2]
            */
            int posOfLen1 = bufHeader.position();
            bufHeader.putInt(0);
            bufHeader.put((byte) 0);
            int tmp1 = bufHeader.position();
            bufHeader.put(headerString.getBytes(StandardCharsets.UTF_8));
            int lengthJsonHeaderString = bufHeader.position() - tmp1 + 1;
            bufHeader.putInt(posOfLen1, lengthJsonHeaderString);
            bufHeader.putInt(lengthJsonHeaderString);
        }

        int CONSTOFF = 0;
        int len2 = Byte.BYTES + 2 * Long.BYTES + valueLength + CONSTOFF;
        bufHeader.putInt(len2);
        bufHeader.put((byte)1);
        bufHeader.putLong(blob.ts);
        bufHeader.putLong(blob.pulseid);
        bufHeader.flip();

        ByteBuffer valueBuf = blob.blobBuf.slice();
        valueBuf.position(valueOffset);
        valueBuf.limit(valueOffset + valueLength + CONSTOFF);
        if (valueBuf.remaining() != valueLength) {
            LOGGER.error("valueBuf.remaining() != valueLength   {} vs {}", valueBuf.remaining(), valueLength);
            throw new RuntimeException("valueBuf.remaining() != valueLength");
        }

        ByteBuffer len2copy = Utils.allocateByteBuffer(Integer.BYTES);
        len2copy.putInt(len2);
        len2copy.flip();

        return Flux.just(bufHeader, valueBuf, len2copy);
    }

    public static ByteBuffer allocateByteBuffer(int len) {
        return ByteBuffer.allocate(len);
    }

}
