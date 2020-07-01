package ch.psi.daq.imageapi.eventmap.ts;

import ch.psi.daq.imageapi.TrailingMapper;
import ch.qos.logback.classic.Logger;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class EventBlobToV1MapTs implements TrailingMapper<Item> {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger("EventBlobToV1MapTs");
    LoggerS LOGGER2;
    static final int HEADER_A_LEN = 2 * Integer.BYTES + 4 * Long.BYTES + 2 * Byte.BYTES;
    DataBufferFactory bufFac;
    DataBuffer left;
    int bufferSize;
    int bufferSize2;
    State state;
    int needMin;
    int missingBytes;
    long pulse;
    long ts;
    int blobLength;
    int headerLength;
    int optionalFieldsLength;
    long endNanos;
    Item item;
    ItemP itemP;
    int maxItemElements;
    String name;
    int termApplyCount;
    long itemCount;
    int contextId;

    static class LoggerS {
        Logger logger;
        public LoggerS(String name) {
            logger = (Logger) LoggerFactory.getLogger("EventBlobToV1MapTs_"+name);
            logger.setLevel(LOGGER.getEffectiveLevel());
        }
        public void trace(String fmt) {
            logger.trace(fmt);
        }
        public void trace(String fmt, Object... args) {
            logger.trace(fmt, args);
        }
        public void debug(String fmt) {
            logger.debug(fmt);
        }
        public void debug(String fmt, Object... args) {
            logger.debug(fmt, args);
        }
        public void info(String fmt) {
            logger.info(fmt);
        }
        public void info(String fmt, Object... args) {
            logger.info(fmt, args);
        }
        public void warn(String fmt, Object... args) {
            logger.warn(fmt, args);
        }
        public void error(String fmt, Object... args) {
            logger.error(fmt, args);
        }
    }

    enum State {
        EXPECT_HEADER_A,
        EXPECT_BLOBS,
        EXPECT_SECOND_LENGTH,
        TERM,
    }

    public EventBlobToV1MapTs(String name, long endNanos, DataBufferFactory bufferFactory, int bufferSize) {
        this.name = name;
        this.LOGGER2 = new LoggerS(name);
        this.bufFac = bufferFactory;
        this.bufferSize = bufferSize;
        this.bufferSize2 = 2 * bufferSize;
        this.endNanos = endNanos;
        this.maxItemElements = 4;
        this.state = State.EXPECT_HEADER_A;
        this.needMin = HEADER_A_LEN;
    }

    public static Function<Flux<DataBuffer>, Publisher<Item>> trans(String name, long endNanos, DataBufferFactory bufFac, int bufferSize, int contextId) {
        EventBlobToV1MapTs mapper = new EventBlobToV1MapTs(name, endNanos, bufFac, bufferSize);
        mapper.contextId = contextId;
        return fl -> {
            return fl
            .map(item -> {
                try {
                    return mapper.apply(item);
                }
                catch (Throwable e) {
                    LOGGER.error("ERROR IN MAPPER {}", e.toString());
                    throw new RuntimeException(e);
                }
            })
            .concatWith(Mono.defer(() -> Mono.just(mapper.lastResult())))
            .doOnTerminate(mapper::release);
        };
    }

    @Override
    public Item apply(DataBuffer buf) {
        if (buf.readableByteCount() == 0) {
            LOGGER2.warn("empty byte buffer received");
            //throw new RuntimeException("empty byte buffer received " + name);
        }
        LOGGER2.trace("apply  buf rp {}  buf wp {}  buf n {}", buf.readPosition(), buf.writePosition(), buf.readableByteCount());
        if (state == State.TERM) {
            if (termApplyCount == 0) {
                LOGGER2.trace("apply buffer despite TERM  c: {}", termApplyCount);
            }
            else {
                LOGGER2.warn("apply buffer despite TERM  c: {}", termApplyCount);
            }
            termApplyCount += 1;
            item = new Item();
            itemCount += 1;
            item.term = true;
            return item;
        }
        else {
            Item ret = apply2(buf);
            LOGGER2.trace("return Item  has item1 {}", ret.item1 != null);
            return ret;
        }
    }

    public Item apply2(DataBuffer buf) {
        if (left == null) {
            item = new Item();
            itemCount += 1;
            item.item1 = new ItemP();
            itemP = item.item1;
            itemP.c = 0;
            itemP.ts = new long[maxItemElements];
            itemP.pos = new int[maxItemElements];
            itemP.ty = new int[maxItemElements];
            itemP.len = new int[maxItemElements];
            itemP.buf = buf;
            itemP.p1 = buf.readPosition();
        }
        else {
            item = new Item();
            itemCount += 1;
            if (needMin <= 0) {
                throw new RuntimeException("logic");
            }
            if (left.readableByteCount() <= 0) {
                throw new RuntimeException("logic");
            }
            if (left.readableByteCount() >= needMin) {
                throw new RuntimeException("logic");
            }
            int n = Math.min(buf.readableByteCount(), needMin - left.readableByteCount());
            int l1 = buf.readableByteCount();
            int l2 = left.readableByteCount();
            left.write(buf.slice(buf.readPosition(), n));
            buf.readPosition(buf.readPosition() + n);
            if (buf.readableByteCount() + n != l1) {
                throw new RuntimeException("logic");
            }
            if (left.readableByteCount() != l2 + n) {
                throw new RuntimeException("logic");
            }
            if (left.readableByteCount() >= needMin) {
                itemP = new ItemP();
                item.item1 = itemP;
                itemP.c = 0;
                itemP.ts = new long[maxItemElements];
                itemP.pos = new int[maxItemElements];
                itemP.ty = new int[maxItemElements];
                itemP.len = new int[maxItemElements];
                itemP.p1 = left.readPosition();
                parse(left);
                if (left.readableByteCount() != 0) {
                    throw new RuntimeException("logic");
                }
                itemP.p2 = left.readPosition();
                itemP.buf = left;
                left = null;
                itemP.buf.readPosition(itemP.p1);
                itemP.buf.writePosition(itemP.p2);
                itemP = new ItemP();
                item.item2 = itemP;
                itemP.c = 0;
                itemP.ts = new long[maxItemElements];
                itemP.pos = new int[maxItemElements];
                itemP.ty = new int[maxItemElements];
                itemP.len = new int[maxItemElements];
                itemP.p1 = buf.readPosition();
            }
            else {
                if (buf.readableByteCount() != 0) {
                    throw new RuntimeException("logic");
                }
                itemP = new ItemP();
                item.item1 = itemP;
                itemP.c = 0;
                itemP.ts = new long[maxItemElements];
                itemP.pos = new int[maxItemElements];
                itemP.ty = new int[maxItemElements];
                itemP.len = new int[maxItemElements];
                itemP.p1 = buf.readPosition();
                itemP.p2 = buf.readPosition();
            }
        }
        while (true) {
            if (state == State.TERM) {
                break;
            }
            if (buf.readableByteCount() == 0 || buf.readableByteCount() < needMin) {
                break;
            }
            parse(buf);
        }
        if (state != State.TERM && buf.readableByteCount() > 0) {
            if (needMin <= 0) {
                throw new RuntimeException("logic");
            }
            if (buf.readableByteCount() >= needMin) {
                throw new RuntimeException("logic");
            }
            itemP.p2 = buf.readPosition();
            left = bufFac.allocateBuffer(bufferSize2);
            left.write(buf.slice(buf.readPosition(), buf.readableByteCount()));
            buf.readPosition(buf.writePosition());
            itemP.buf = buf;
            itemP.buf.readPosition(itemP.p1);
            itemP.buf.writePosition(itemP.p2);
        }
        else {
            itemP.p2 = buf.readPosition();
            itemP.buf = buf;
            itemP.buf.readPosition(itemP.p1);
            itemP.buf.writePosition(itemP.p2);
        }
        Item ret = item;
        item = null;
        return ret;
    }

    void parse(DataBuffer buf) {
        if (buf == null) {
            throw new RuntimeException("logic");
        }
        if (state == State.EXPECT_HEADER_A) {
            parseHeaderA(buf);
        }
        else if (state == State.EXPECT_BLOBS) {
            parseBlob(buf);
        }
        else if (state == State.EXPECT_SECOND_LENGTH) {
            parseSecondLength(buf);
        }
        else if (state == State.TERM) {
        }
        else {
            throw new RuntimeException("logic");
        }
    }

    void parseHeaderA(DataBuffer buf) {
        int bpos = buf.readPosition();
        ByteBuffer bb = buf.asByteBuffer(buf.readPosition(), needMin);
        buf.readPosition(buf.readPosition() + needMin);
        int length = bb.getInt();
        if (length == 0) {
            LOGGER2.warn("Stop because length == 0");
            state = State.TERM;
            return;
        }
        if (length < 40 || length > 60 * 1024 * 1024) {
            LOGGER2.error(String.format("Stop because unexpected length: %d", length));
            state = State.TERM;
            return;
        }
        long ttl = bb.getLong();
        long ts = bb.getLong();
        long pulse = bb.getLong();
        long iocTime = bb.getLong();
        byte status = bb.get();
        byte severity = bb.get();
        LOGGER2.trace("seen  length  {}  timestamp {} {}  pulse {}", length, ts / 1000000000L, ts % 1000000000, pulse);
        if (ts >= endNanos) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER2.warn("ts >= endNanos  {} {}  >=  {} {}   item {}", ts / 1000000000L, ts % 1000000000, endNanos / 1000000000L, endNanos % 1000000000, item);
            }
            else {
                LOGGER2.warn("ts >= endNanos  {} {}  >=  {} {}", ts / 1000000000L, ts % 1000000000, endNanos / 1000000000L, endNanos % 1000000000);
            }
            state = State.TERM;
            buf.readPosition(bpos);
            buf.writePosition(bpos);
            return;
        }
        int optionalFieldsLength = bb.getInt();
        if (optionalFieldsLength < -1 || optionalFieldsLength == 0) {
            LOGGER2.error("unexpected value for optionalFieldsLength: {}", optionalFieldsLength);
            throw new RuntimeException("unexpected optional fields");
        }
        if (optionalFieldsLength != -1) {
            LOGGER2.warn("Found optional fields: {}", optionalFieldsLength);
        }
        if (optionalFieldsLength > 2048) {
            LOGGER2.error("unexpected optional fields: {}", optionalFieldsLength);
            throw new RuntimeException("unexpected optional fields");
        }
        headerLength = needMin;
        this.pulse = pulse;
        this.ts = ts;
        this.optionalFieldsLength = Math.max(optionalFieldsLength, 0);
        this.blobLength = length;
        reallocItem();
        itemP.ts[itemP.c] = ts;
        itemP.pos[itemP.c] = bpos;
        itemP.ty[itemP.c] = 1;
        itemP.len[itemP.c] = this.blobLength;
        itemP.c += 1;
        state = State.EXPECT_BLOBS;
        missingBytes = length - needMin - 4;
        needMin = 0;
    }

    void parseBlob(DataBuffer buf) {
        final int n = Math.min(buf.readableByteCount(), missingBytes);
        buf.readPosition(buf.readPosition() + n);
        missingBytes -= n;
        if (missingBytes > 0) {
            needMin = 0;
        }
        else {
            state = State.EXPECT_SECOND_LENGTH;
            needMin = Integer.BYTES;
        }
    }

    void parseSecondLength(DataBuffer buf) {
        int bpos = buf.readPosition();
        ByteBuffer bb = buf.asByteBuffer(buf.readPosition(), needMin);
        buf.readPosition(buf.readPosition() + needMin);
        int len2 = bb.getInt();
        if (len2 == -1) {
            LOGGER2.warn("2nd length -1 encountered, ignoring");
        }
        else if (len2 == 0) {
            LOGGER2.warn("2nd length 0 encountered, ignoring");
        }
        else if (len2 != blobLength) {
            LOGGER2.error("event blob length mismatch at {}   {} vs {}", buf.readPosition(), len2, blobLength);
            throw new RuntimeException("unexpected 2nd length");
        }
        reallocItem();
        itemP.ts[itemP.c] = ts;
        itemP.pos[itemP.c] = bpos + 4;
        itemP.ty[itemP.c] = 2;
        itemP.len[itemP.c] = -1;
        itemP.c += 1;
        state = State.EXPECT_HEADER_A;
        needMin = HEADER_A_LEN;
    }

    void reallocItem() {
        if (itemP.c >= maxItemElements) {
            if (maxItemElements >= 128 * 1024) {
                throw new RuntimeException("too many elements");
            }
            int n1 = maxItemElements;
            maxItemElements *= 4;
            long[] ts2 = itemP.ts;
            int[] pos2 = itemP.pos;
            int[] ty2 = itemP.ty;
            int[] len2 = itemP.len;
            itemP.ts = new long[maxItemElements];
            itemP.pos = new int[maxItemElements];
            itemP.ty = new int[maxItemElements];
            itemP.len = new int[maxItemElements];
            System.arraycopy(ts2, 0, itemP.ts, 0, n1);
            System.arraycopy(pos2, 0, itemP.pos, 0, n1);
            System.arraycopy(ty2, 0, itemP.ty, 0, n1);
            System.arraycopy(len2, 0, itemP.len, 0, n1);
        }
    }

    public void release() {
        LOGGER2.debug("EventBlobToV1MapTs release");
        if (left != null) {
            DataBufferUtils.release(left);
            left = null;
        }
    }

    @Override
    public Item lastResult() {
        DataBuffer buf = bufFac.allocateBuffer(bufferSize);
        item = apply(buf);
        item.isLast = true;
        return item;
    }

}
