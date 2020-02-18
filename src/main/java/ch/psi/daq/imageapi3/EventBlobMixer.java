package ch.psi.daq.imageapi3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class EventBlobMixer {
    static Logger LOGGER = LoggerFactory.getLogger(EventBlobMixer.class);
    List<PositionedDatafile> files;
    List<Optional<PositionedDatafile.Blob>> blobs;
    long beginNano;
    long endNano;
    int sunkenBlobs;
    long sunkenBytes;
    long lastSunkenTs = -1;

    public static EventBlobMixer create(List<PositionedDatafile> files, long beginNano, long endNano) {
        List<Optional<PositionedDatafile.Blob>> blobs = new ArrayList<>();
        for (PositionedDatafile file : files) {
            blobs.add(file.getNext());
        }
        EventBlobMixer ret = new EventBlobMixer();
        ret.files = files;
        ret.blobs = blobs;
        ret.beginNano = beginNano;
        ret.endNano = endNano;
        return ret;
    }

    public EventBlobMixer sinkNext(SynchronousSink<PositionedDatafile.Blob> sink) {
        //LOGGER.info("EventBlobMixer sinkNext  thread: {}", Thread.currentThread().getName());
        if (sunkenBytes > 500L * (1L<<30)) {
            LOGGER.error("abort after {} blobs", sunkenBytes);
            sink.error(new RuntimeException("at sinkNext limit"));
            return this;
        }
        long tsMin = Long.MAX_VALUE;
        int ixMin = -1;
        for (int i = 0; i < blobs.size(); i+=1) {
            Optional<PositionedDatafile.Blob> ob = blobs.get(i);
            if (ob.isPresent()) {
                PositionedDatafile.Blob blob = ob.get();
                //LOGGER.info("consider {}", blob.ts);
                if (blob.ts < tsMin) {
                    ixMin = i;
                    tsMin = blob.ts;
                    //LOGGER.info("new tsMin: {}  ixMin: {}", tsMin, ixMin);
                }
            }
        }
        if (ixMin >= 0) {
            if (tsMin < lastSunkenTs) {
                LOGGER.error("encountered out of sequence ts: {}", tsMin);
                sink.error(new RuntimeException("unexpected data"));
            }
            else if (tsMin < beginNano) {
                LOGGER.error("encountered too early ts: {}", tsMin);
                sink.error(new RuntimeException("unexpected data"));
            }
            else if (tsMin < endNano) {
                PositionedDatafile.Blob blobMin = blobs.get(ixMin).get();
                blobs.set(ixMin, files.get(ixMin).getNext());
                long blobNBytes = blobMin.blobBuf.remaining();
                //LOGGER.info("sinking ts: {}  ixMin: {}", tsMin, ixMin);
                sink.next(blobMin);
                sunkenBlobs += 1;
                sunkenBytes += blobNBytes;
            }
            else {
                sink.complete();
            }
        }
        else {
            LOGGER.info("completing because exhausted");
            sink.complete();
        }
        return this;
    }

}
