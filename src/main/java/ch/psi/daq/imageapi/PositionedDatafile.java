package ch.psi.daq.imageapi;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class PositionedDatafile {

    final SeekableByteChannel channel;
    Path path;

    PositionedDatafile(SeekableByteChannel channel, Path path) {
        this.path = path;
        this.channel = channel;
    }

    /**
     * @param channel The byte channel.
     * @param path For error handling, remember the path of the byte channel.
     */
    public static PositionedDatafile fromChannel(SeekableByteChannel channel, Path path) {
        return new PositionedDatafile(channel, path);
    }

    public static Mono<PositionedDatafile> openAndPosition(Path path, String channelName, long beginNano) {
        return Index.openIndex(Path.of(path.toString() + "_Index"))
        .map(x -> Index.findGEByLong(beginNano, x))
        .flatMap(x -> {
            return Mono.fromCallable(() -> {
                SeekableByteChannel c = Files.newByteChannel(path, StandardOpenOption.READ);
                // TODO verify correct channel name here
                if (x.v >= c.size()) {
                    throw Utils.SeekError.empty();
                }
                c.position(x.v);
                return PositionedDatafile.fromChannel(c, path);
            });
        })
        .subscribeOn(Schedulers.boundedElastic());
    }

}
