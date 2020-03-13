package ch.psi.daq.imageapi;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public class Utils {

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
