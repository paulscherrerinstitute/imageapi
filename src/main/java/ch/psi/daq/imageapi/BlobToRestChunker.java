package ch.psi.daq.imageapi;

import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;

public class BlobToRestChunker {

    long count = 0;

    synchronized public Flux<ByteBuffer> chunk(String channelName, PositionedDatafile.Blob blob) {
        Flux<ByteBuffer> ret = Utils.blobToRestChunk(channelName, blob, count == 0);
        count += 1;
        return ret;
    }

}
