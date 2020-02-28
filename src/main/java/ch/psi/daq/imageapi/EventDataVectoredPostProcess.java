package ch.psi.daq.imageapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

class EventDataVectoredPostProcess {
    static Logger LOGGER = LoggerFactory.getLogger(EventDataVectoredPostProcess.class);
    ChannelWithFiles channelWithFiles;
    RequestStats requestStats;
    ObjectWriter ow = new ObjectMapper().writer();
    EventDataVectoredPostProcess(ChannelWithFiles channelWithFiles, RequestStats requestStats) {
        this.channelWithFiles = channelWithFiles;
        this.requestStats = requestStats;
    }
    int i1 = 0;
    void postProcess(EventDataVectored ev) {
        if (ev.blobs.size() <= 0) {
            LOGGER.error("No blobs in the EventDataVectored  channel: {}", channelWithFiles.name);
            return;
        }
        DataBufferFactory bufFac = ev.blobs.get(0).factory();
        if (i1 <= 0) {
            Utils.BlobJsonHeader header = new Utils.BlobJsonHeader();
            header.name = channelWithFiles.name;
            header.type = DTypeBitmapUtils.Type.lookup(ev.dtype).toString().toLowerCase();
            header.compression = String.format("%d", ev.compressionForRetrieval());
            header.byteOrder = ev.byteOrder.toString();
            header.shape = new ArrayList<>();
            for (int i = 0; i < ev.shapeDims; i++) {
                header.shape.add(ev.shapeSizes[i]);
            }
            String headerString;
            try {
                headerString = ow.writeValueAsString(header);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException("JsonProcessingException");
            }
            //LOGGER.info("headerString: {}", headerString);
            ByteBuffer headerEncoded = StandardCharsets.UTF_8.encode(headerString);
            int nh = headerEncoded.remaining();
            //LOGGER.info("headerString: {}  nh: {}", headerString.length(), nh);
            ByteBuffer bb1 = ByteBuffer.allocate(1024);
            bb1.putInt(0);
            bb1.put((byte)0);
            bb1.put(headerEncoded);
            if (bb1.position() != 5 + nh) {
                throw new RuntimeException("unexpected");
            }
            bb1.putInt(0, 1 + nh);
            bb1.putInt(1 + nh);
            int vtot = ev.totalValueLength();
            //LOGGER.info("ev.totalValueLength() {}", vtot);
            bb1.putInt(17 + vtot);
            bb1.put((byte)1);
            bb1.putLong(ev.ts);
            bb1.putLong(ev.pulse);
            bb1.flip();
            ByteBuffer bb2 = ByteBuffer.allocate(1024);
            bb2.putInt(17 + vtot);
            bb2.flip();
            ArrayList<DataBuffer> framed = new ArrayList<>();
            framed.add(bufFac.wrap(bb1));
            framed.addAll(ev.blobs);
            framed.add(bufFac.wrap(bb2));
            ev.blobs.clear();
            ev.blobs = framed;
        }
        else {
            int vtot = ev.totalValueLength();
            //LOGGER.info("ev.totalValueLength() {}", vtot);
            ByteBuffer bb1 = ByteBuffer.allocate(1024);
            bb1.putInt(17 + vtot);
            bb1.put((byte)1);
            bb1.putLong(ev.ts);
            bb1.putLong(ev.pulse);
            bb1.flip();
            ByteBuffer bb2 = ByteBuffer.allocate(1024);
            bb2.putInt(17 + vtot);
            bb2.flip();
            ArrayList<DataBuffer> framed = new ArrayList<>();
            framed.add(bufFac.wrap(bb1));
            framed.addAll(ev.blobs);
            framed.add(bufFac.wrap(bb2));
            ev.blobs.clear();
            ev.blobs = framed;
        }
        i1++;
    }
}
