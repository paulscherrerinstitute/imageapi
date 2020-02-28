package ch.psi.daq.imageapi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;

enum State {
    EXPECT_HEADER_A,
    EXPECT_HEADER_B,
    EXPECT_HEADER_C,
    EXPECT_HEADER_D,
    EXPECT_BLOBS,
    EXPECT_SECOND_LENGTH,
    TERM,
}

class BufPos {
    DataBuffer buf;
    long beginAbsPos;
    int beginPos;
    byte isLeftover;
    void release() {
        DataBufferUtils.release(buf);
    }
    public String toString() {
        return String.format("beginAbsPos: %16x  beginPos: %8x  isLeftover: %d", beginAbsPos, beginPos, isLeftover);
    }
}

class PositionedDataBufferFluxGenerator {
    static Logger LOGGER = LoggerFactory.getLogger(PositionedDataBufferFluxGenerator.class);
    State state = State.EXPECT_HEADER_A;
    final int HEADER_A_LEN = 2 * Integer.BYTES + 4 * Long.BYTES + 2 * Byte.BYTES;
    int expectMin = HEADER_A_LEN;
    EventDataVectored currentEvent;
    BufPos leftover = null;
    int missingBlobLength;
    int channelId;
    long absPos;

    PositionedDataBufferFluxGenerator(int channelId, long absPos) {
        this.channelId = channelId;
        this.absPos = absPos;
    }

    List<EventDataVectored> map(DataBuffer buf) {
        try {
            long beginAbsPos = absPos;
            absPos += buf.readableByteCount();
            int beginPos = buf.readPosition();
            BufPos bufPos = new BufPos();
            bufPos.buf = buf;
            bufPos.beginAbsPos = beginAbsPos;
            bufPos.beginPos = beginPos;
            if (false) {
                LOGGER.info("map: {}", bufPos.toString());
            }
            return mapThrowing(bufPos);
        }
        finally {
            DataBufferUtils.release(buf);
        }
    }

    List<EventDataVectored> mapThrowing(BufPos bufPos) {
        if (state == State.TERM) {
            LOGGER.info("Return TERM");
            return List.of(EventDataVectored.term());
        }
        DataBuffer buf = bufPos.buf;
        List<EventDataVectored> events = new ArrayList<>();
        if (leftover != null) {
            if (expectMin > 128) {
                LOGGER.error("unexpected large expectMin: {}", expectMin);
                throw new RuntimeException("logic error");
            }
            if (expectMin <= 0) {
                LOGGER.error("leftover but no expectMin: {}", expectMin);
                throw new RuntimeException("logic error");
            }
            if (expectMin <= leftover.buf.readableByteCount()) {
                LOGGER.error("expectMin <= leftover.readableByteCount()  {} <= {}", expectMin, leftover.buf.readableByteCount());
                throw new RuntimeException("logic error");
            }
            int n = Math.min(expectMin - leftover.buf.readableByteCount(), buf.readableByteCount());
            if (leftover.buf.writableByteCount() < n) {
                LOGGER.error("leftover.writableByteCount() < n :    {} < {}", leftover.buf.writableByteCount(), n);
                throw new RuntimeException("logic error");
            }
            leftover.buf.write(buf.slice(buf.readPosition(), n));
            buf.readPosition(buf.readPosition() + n);
            loop(leftover, events);
            if (buf.readableByteCount() > 0) {
                if (leftover.buf.readableByteCount() > 0) {
                    throw new RuntimeException("logic error");
                }
                leftover.release();
                leftover = null;
                process(bufPos, events);
            }
        }
        else {
            process(bufPos, events);
        }
        if (state == State.TERM) {
            LOGGER.info("add TERM");
            events.add(EventDataVectored.term());
        }
        return events;
    }

    void process(BufPos bufPos, List<EventDataVectored> events) {
        DataBuffer buf = bufPos.buf;
        if (leftover != null) {
            throw new RuntimeException("have leftover");
        }
        loop(bufPos, events);
        if (state == State.TERM) {
            return;
        }
        if (buf.readableByteCount() > 0) {
            if (expectMin <= 0) {
                throw new RuntimeException("leftover without expect");
            }
            if (buf.readableByteCount() > 128) {
                throw new RuntimeException("unexpected parse");
            }
            leftover = new BufPos();
            leftover.isLeftover = 1;
            leftover.buf = buf.factory().wrap(new byte[1024]);
            if (leftover.buf.readPosition() != 0) {
                throw new RuntimeException("expect empty buffer");
            }
            leftover.beginAbsPos = bufPos.beginAbsPos + bufPos.buf.readPosition();
            leftover.buf.writePosition(0);
            leftover.buf.write(buf);
            leftover.beginPos = leftover.buf.readPosition();
            buf.readPosition(buf.writePosition());
        }
    }

    void loop(BufPos bufPos, List<EventDataVectored> events) {
        DataBuffer buf = bufPos.buf;
        int i1 = 0;
        while (buf.readableByteCount() > 0 && buf.readableByteCount() >= expectMin) {
            if (state == State.EXPECT_HEADER_A) {
                readHeaderA(bufPos);
            }
            else if (state == State.EXPECT_HEADER_B) {
                readHeaderB(bufPos);
            }
            else if (state == State.EXPECT_HEADER_C) {
                readHeaderC(bufPos);
            }
            else if (state == State.EXPECT_HEADER_D) {
                readHeaderD(bufPos);
            }
            else if (state == State.EXPECT_BLOBS) {
                readBlob(bufPos);
            }
            else if (state == State.EXPECT_SECOND_LENGTH) {
                if (readSecondLength(bufPos)) {
                    events.add(currentEvent);
                    currentEvent = null;
                    state = State.EXPECT_HEADER_A;
                    expectMin = HEADER_A_LEN;
                }
            }
            else if (state == State.TERM) {
                break;
            }
            else {
                LOGGER.error(String.format("Unknown state  %s", state.toString()));
                throw new RuntimeException("Unknown state");
            }
            i1 += 1;
            if (i1 > 1000) {
                throw new RuntimeException("unexpected large blob");
            }
        }
    }

    void readHeaderA(BufPos bufPos) {
        DataBuffer buf = bufPos.buf;
        if (buf.readableByteCount() < HEADER_A_LEN) {
            throw new RuntimeException("short input");
        }
        ByteBuffer bb = buf.asByteBuffer();
        buf.readPosition(buf.readPosition() + HEADER_A_LEN);

        byte[] headerCopy = new byte[128];
        {
            int i2 = 0;
            for (int i1 = bb.position(); i1 < bb.position() + bb.remaining() && i2 < headerCopy.length; ++i1) {
                headerCopy[i2] = bb.get(i1);
                ++i2;
            }
        }

        int length = bb.getInt();

        if (length == 0) {
            LOGGER.info("Stop because length == 0");
            state = State.TERM;
            return;
        }

        if (length < 0 || length > 60 * 1024 * 1024) {
            LOGGER.error(String.format("Stop because unexpected length: %d  bufPos: %s", length, bufPos.toString()));
            state = State.TERM;
            return;
        }

        long ttl = bb.getLong();
        long ts = bb.getLong();
        long pulse = bb.getLong();
        long iocTime = bb.getLong();
        byte status = bb.get();
        byte severity = bb.get();
        int optionalFieldsLength = bb.getInt();
        if (optionalFieldsLength < -1 || optionalFieldsLength == 0) {
            LOGGER.error("unexpected value for optionalFieldsLength: {}", optionalFieldsLength);
            throw new RuntimeException("unexpected optional fields");
        }
        if (optionalFieldsLength != -1) {
            LOGGER.warn("Found optional fields: {}", optionalFieldsLength);
        }
        currentEvent = new EventDataVectored();
        currentEvent.headerCopy = headerCopy;
        currentEvent.channelId = channelId;
        currentEvent.ts = ts;
        currentEvent.pulse = pulse;
        currentEvent.optionalFieldsLength = Math.max(optionalFieldsLength, 0);
        currentEvent.headerLength = HEADER_A_LEN - Integer.BYTES;
        currentEvent.totalLength = length;
        if (optionalFieldsLength > 2048) {
            LOGGER.error("unexpected optional fields: {}\n" + currentEvent.formatCopy(), optionalFieldsLength);
            throw new RuntimeException("unexpected optional fields");
        }
        state = State.EXPECT_HEADER_B;
        expectMin = currentEvent.optionalFieldsLength + Short.BYTES;
    }

    void readHeaderB(BufPos bufPos) {
        DataBuffer buf = bufPos.buf;
        ByteBuffer bb = buf.asByteBuffer();
        if (bb.position() != 0) {
            LOGGER.warn("readHeaderB test position {}, {}", bb.position(), bufPos.toString());
        }
        buf.readPosition(buf.readPosition() + expectMin);
        currentEvent.headerLength += expectMin;
        bb.position(bb.position() + currentEvent.optionalFieldsLength);
        expectMin = 0;
        int dtypeBitmask = 0xffff & bb.getShort();
        currentEvent.dtype = (short) (0xff & dtypeBitmask);
        if (currentEvent.dtype > 13) {
            LOGGER.error("unexpected datatype: {}", currentEvent.dtype);
            throw new RuntimeException("unexpected datatype");
        }
        if ((dtypeBitmask & 0x8000) != 0) {
            currentEvent.hasCompression = true;
            expectMin += Byte.BYTES;
        }
        if ((dtypeBitmask & 0x2000) != 0) {
            currentEvent.byteOrder = ByteOrder.BIG_ENDIAN;
        }
        if ((dtypeBitmask & 0x1000) != 0) {
            currentEvent.hasShape = true;
            expectMin += Byte.BYTES;
        }
        state = State.EXPECT_HEADER_C;
    }

    void readHeaderC(BufPos bufPos) {
        DataBuffer buf = bufPos.buf;
        ByteBuffer bb = buf.asByteBuffer();
        buf.readPosition(buf.readPosition() + expectMin);
        currentEvent.headerLength += expectMin;
        expectMin = 0;
        if (currentEvent.hasCompression) {
            currentEvent.compressionMethod = bb.get();
            if (currentEvent.compressionMethod < 0 || currentEvent.compressionMethod > 1) {
                LOGGER.error("unknown compressionMethod: {}  ts: {}", currentEvent.compressionMethod, currentEvent.ts);
            }
        }
        if (currentEvent.hasShape) {
            currentEvent.shapeDims = 0xff & bb.get();
            expectMin = currentEvent.shapeDims * Integer.BYTES;
        }
        if (currentEvent.shapeDims > 3) {
            LOGGER.info("currentEvent.shapeDims > 3  this event blob: {}", currentEvent.formatCopy());
            throw new RuntimeException("unexpected shapeDims");
        }
        state = State.EXPECT_HEADER_D;
    }

    void readHeaderD(BufPos bufPos) {
        DataBuffer buf = bufPos.buf;
        ByteBuffer bb = buf.asByteBuffer();
        buf.readPosition(buf.readPosition() + expectMin);
        currentEvent.headerLength += expectMin;
        missingBlobLength = currentEvent.totalLength - currentEvent.headerLength - 2 * Integer.BYTES;
        for (int i = 0; i < currentEvent.shapeDims; i++) {
            int n = bb.getInt();
            currentEvent.shapeSizes[i] = n;
        }
        expectMin = 0;
        state = State.EXPECT_BLOBS;
    }

    void readBlob(BufPos bufPos) {
        DataBuffer buf = bufPos.buf;
        if (buf.readableByteCount() <= 0) {
            throw new RuntimeException("unexpected input");
        }
        final int n = Math.min(buf.readableByteCount(), missingBlobLength);
        //currentEvent.blobs.add(buf.retainedSlice(buf.readPosition(), n));
        DataBuffer buf2 = buf.slice(buf.readPosition(), n);
        ByteBuffer bb1 = ByteBuffer.allocate(n);
        DataBuffer buf3 = buf.factory().wrap(bb1);
        buf3.writePosition(0);
        buf3.write(buf2);
        buf.readPosition(buf.readPosition() + n);
        currentEvent.blobs.add(buf3);
        missingBlobLength -= n;
        if (missingBlobLength <= 0) {
            state = State.EXPECT_SECOND_LENGTH;
            expectMin = Integer.BYTES;
        }
        else {
            // collect stats
        }
    }

    boolean readSecondLength(BufPos bufPos) {
        DataBuffer buf = bufPos.buf;
        int len2 = buf.asByteBuffer().getInt();
        buf.readPosition(buf.readPosition() + Integer.BYTES);
        if (len2 != -1 && len2 != 0 && len2 != currentEvent.totalLength) {
            LOGGER.error("event blob length mismatch  {} vs {}", len2, currentEvent.totalLength);
            throw new RuntimeException("unexpected 2nd length");
        }
        return true;
    }

    synchronized void doOnComplete() {
        //LOGGER.warn("PositionedDataBufferFluxGenerator  doOnComplete");
        if (leftover != null) {
            leftover.release();
            leftover = null;
        }
    }

    synchronized void doOnError(Throwable e) {
        LOGGER.warn("PositionedDataBufferFluxGenerator  doOnError:", e);
        if (leftover != null) {
            leftover.release();
            leftover = null;
        }
    }

}

public class PositionedDataBufferFlux {
    static Logger LOGGER = LoggerFactory.getLogger(PositionedDataBufferFlux.class);
    final Flux<DataBuffer> bufferFlux;
    int channelId;
    ReadableByteChannel channel;
    long begin;
    PositionedDataBufferFluxGenerator generator;

    PositionedDataBufferFlux(ReadableByteChannel channel, Flux<DataBuffer> bufferFlux, int channelId, long begin) {
        this.bufferFlux = bufferFlux;
        this.channelId = channelId;
        this.channel = channel;
        this.begin = begin;
        generator = new PositionedDataBufferFluxGenerator(channelId, begin);
    }

    public static PositionedDataBufferFlux create(ReadableByteChannel channel, Flux<DataBuffer> bufferFlux, int channelId, long begin) {
        return new PositionedDataBufferFlux(channel, bufferFlux, channelId, begin);
    }

    public Flux<EventDataVectored> eventDataFlux() {
        return bufferFlux
        .doOnError(java.nio.channels.ClosedChannelException.class, e -> LOGGER.error("EH 2: {}", e.toString()))
        .doOnError(x -> LOGGER.error("EH 2 byte channel buffer flux doOnError"))
        .onErrorStop()
        .doOnDiscard(DataBuffer.class, x -> {
            LOGGER.info("releasing buf cap: {}", x.capacity());
            DataBufferUtils.release(x);
        })
        .doOnDiscard(Object.class, x -> { LOGGER.info("unmapped buffer relase as Object"); })
        //.doOnTerminate(() -> LOGGER.info("data buffer flux terminated"))
        //.doOnComplete(() -> LOGGER.warn("data buffer flux COMPLETE"))
        .map(generator::map)
        .flatMapSequential(Flux::fromIterable)
        .doOnComplete(generator::doOnComplete)
        .doOnError(generator::doOnError);
    }

    public void doOnComplete() {
        generator.doOnComplete();
    }

}
