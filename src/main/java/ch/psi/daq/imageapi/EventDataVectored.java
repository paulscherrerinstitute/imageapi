package ch.psi.daq.imageapi;

import com.google.common.io.BaseEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;

import java.nio.ByteOrder;
import java.util.ArrayList;

public class EventDataVectored {
    static Logger LOGGER = LoggerFactory.getLogger(EventDataVectored.class);

    public long ts;
    public long pulse;
    public int optionalFieldsLength;
    public short dtype;
    boolean hasCompression;
    boolean hasShape;
    ByteOrder byteOrder = ByteOrder.LITTLE_ENDIAN;
    int shapeDims;
    byte compressionMethod;
    int headerLength;
    int totalLength;
    int[] shapeSizes = new int[8];
    ArrayList<DataBuffer> blobs = new ArrayList<>();
    int channelId;
    byte[] headerCopy;

    public static EventDataVectored term() {
        EventDataVectored ret = new EventDataVectored();
        ret.ts = Long.MIN_VALUE;
        return ret;
    }

    public int totalValueLength() {
        return blobs.stream().map(DataBuffer::readableByteCount).mapToInt(x -> x).sum();
    }

    public int compressionForRetrieval() {
        if (!hasCompression) {
            return 0;
        }
        if (compressionMethod == 0) {
            // bitshuffle lz4
            return 1;
        }
        if (compressionMethod == 1) {
            // lz4
            return 2;
        }
        LOGGER.error("unknown compression: {}, {}", hasCompression, compressionMethod);
        throw new RuntimeException("unknown compression");
    }

    public void release() {
        //LOGGER.info("release a whole event buffer");
        blobs.forEach(DataBufferUtils::release);
        blobs.clear();
    }

    public String formatCopy() {
        String hex = BaseEncoding.base16().lowerCase().encode(headerCopy);
        StringBuilder b = new StringBuilder();
        int i1 = 0;
        b.append(hex.substring(i1, i1+8));
        b.append('\n');
        i1 += 8;
        b.append(hex.substring(i1, i1+16));
        b.append('\n');
        i1 += 16;
        b.append(hex.substring(i1, i1+16));
        b.append('\n');
        i1 += 16;
        b.append(hex.substring(i1, i1+16));
        b.append('\n');
        i1 += 16;
        b.append(hex.substring(i1, i1+16));
        b.append('\n');
        i1 += 16;
        b.append(hex.substring(i1, i1+2));
        b.append('\n');
        i1 += 2;
        b.append(hex.substring(i1, i1+2));
        b.append('\n');
        i1 += 2;
        b.append(hex.substring(i1, i1+8));
        b.append('\n');
        i1 += 8;
        b.append(hex.substring(i1));
        b.append('\n');
        return b.toString();
    }

}
