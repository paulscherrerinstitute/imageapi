package ch.psi.daq.imageapi3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Optional;

public class PositionedDatafile {

    static class Blob {
        ByteBuffer blobBuf;
        long ts;
        long pulseid;
        int lengthNextBlob;
    }

    static Logger LOGGER = LoggerFactory.getLogger(PositionedDatafile.class);
    final SeekableByteChannel channel;
    Path path;
    final long originalPosition;
    Mono<Optional<Blob>> nextBlobMono;
    ArrayList<Integer> blockInstants = new ArrayList<>();
    ArrayList<Integer> blockDurationsMicros = new ArrayList<>();

    PositionedDatafile(SeekableByteChannel channel, Path path) throws IOException {
        this.path = path;
        this.channel = channel;
        this.originalPosition = channel.position();
    }

    public static PositionedDatafile fromChannel(SeekableByteChannel channel, Path path) {
        try {
            PositionedDatafile x = new PositionedDatafile(channel, path);
            // TODO make sure that this is actually dispatched already here in the background.
            x.nextBlobMono = x.lengthOfNextBlob().flatMap(x::nextBlob).subscribeOn(Schedulers.boundedElastic());
            return x;
        }
        catch (IOException e) {
            LOGGER.error("IOException while getting original position");
            return null;
        }
    }

    // TODO currently we throw if the file does not contain any events. Or should we silently accept that?
    Mono<Integer> lengthOfNextBlob() {
        return Mono.fromCallable(() -> {
            synchronized (channel) {
                try {
                    channel.position(originalPosition);
                    ByteBuffer buf = Utils.allocateByteBuffer(4);
                    channel.read(buf);
                    buf.flip();
                    if (buf.limit() < 4) {
                        throw new RuntimeException("unexpected EOF");
                    }
                    return buf.getInt();
                }
                catch (IOException e) {
                    LOGGER.error("IOException");
                    throw new RuntimeException("IOException");
                }
            }
        });
    }

    /*
    `len` is the length of the next blob to read, including the two enclosing int length fields:
    [length][blob][length]
    but our channel is already past the first length field.
    Therefore, we read already the length of the next blob if it exists.
    Otherwise, we get a short read.
    */
    private Mono<Optional<Blob>> nextBlob(int len) {
        if (len < 0 || len > (1<<30)) {
            return Mono.fromCallable(() -> {
                throw new FileFormatException(String.format("unexpected len: %s", len), path, channel.position());
            });
        }
        return Mono.fromCallable(() -> {
            synchronized (channel) {
                ByteBuffer buf = Utils.allocateByteBuffer(len);
                channel.read(buf);
                buf.flip();
                if (buf.limit() + Integer.BYTES < len) {
                    throw new FileFormatException("unexpected EOF a", path, channel.position());
                }
                else {
                    int nextLen = -1;
                    if (buf.limit() == len) {
                        nextLen = buf.getInt(len - Integer.BYTES);
                        buf.limit(buf.limit() - Integer.BYTES);
                    }
                    else if (buf.limit() == len - Integer.BYTES) {
                        LOGGER.info("nextBlob this is the LAST blob in file {}", path);
                    }
                    else {
                        throw new FileFormatException("unexpected EOF b", path, channel.position());
                    }
                    int length2ndOfCurrent = buf.getInt(buf.limit() - Integer.BYTES);
                    if (length2ndOfCurrent != len) {
                        throw new FileFormatException(String.format("length2ndOfCurrent  %d vs %d", len, length2ndOfCurrent), path, channel.position());
                    }
                    long ttl = buf.getLong();
                    long ts = buf.getLong();
                    long pulseid = buf.getLong();
                    //LOGGER.info("ts: {}  pulseid: {}", ts, pulseid);
                    buf.position(0);
                    Blob blob = new Blob();
                    //LOGGER.info("before slice  position: {}  limit: {}", buf.position(), buf.limit());
                    blob.blobBuf = buf.slice();
                    //LOGGER.info("after slice   position: {}  limit: {}", blob.blobBuf.position(), blob.blobBuf.limit());
                    blob.ts = ts;
                    blob.pulseid = pulseid;
                    blob.lengthNextBlob = nextLen;
                    return Optional.of(blob);
                }
            }
        });
    }

    public Optional<Blob> getNext() {
        //LOGGER.info("PositionedDataFile getNext blocking on thread: {}", Thread.currentThread().getName());
        long t1 = System.nanoTime();
        Optional<Blob> blob = nextBlobMono.block();
        long t2 = System.nanoTime();
        long dt = t2 - t1;
        if (blockInstants.size() < 1000000) {
            dt /= 1000;
            if (dt > Integer.MAX_VALUE) {
                dt = Integer.MAX_VALUE;
            }
            blockInstants.add((int) (t1 / 1000000000));
            blockDurationsMicros.add((int) dt);
        }
        if (blob != null && blob.isPresent()) {
            if (blob.get().lengthNextBlob > 0) {
                nextBlobMono = nextBlob(blob.get().lengthNextBlob).subscribeOn(Schedulers.boundedElastic());
            }
            else {
                nextBlobMono = Mono.just(Optional.empty());
            }
        }
        return blob;
    }

}
