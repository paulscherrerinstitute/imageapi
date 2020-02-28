package ch.psi.daq.imageapi;

import com.google.common.io.BaseEncoding;
import org.junit.Test;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDataBuffer {

    @Test
    public void length() {
        DefaultDataBufferFactory fac = new DefaultDataBufferFactory();
        DataBuffer buf = fac.allocateBuffer(1024);
        buf.write("a test data buffer".getBytes(StandardCharsets.UTF_8));
        if (buf.readPosition() != 0) {
            throw new RuntimeException();
        }
        if (buf.writePosition() != 18) {
            throw new RuntimeException();
        }
        ByteBuffer bb = buf.asByteBuffer();
        if (bb.remaining() != 18) {
            throw new RuntimeException();
        }
        buf.readPosition(8);
        if (buf.readableByteCount() != 10) {
            throw new RuntimeException();
        }
        if (bb.remaining() != 18) {
            throw new RuntimeException();
        }
        bb = buf.asByteBuffer();
        if (bb.remaining() != 10) {
            throw new RuntimeException();
        }
    }

    @Test
    public void slice() {
        DefaultDataBufferFactory fac = new DefaultDataBufferFactory();
        DataBuffer buf1 = fac.allocateBuffer(1024);
        DataBuffer buf2 = buf1.retainedSlice(4, 14);
        buf1.write("a test data buffer".getBytes(StandardCharsets.UTF_8));
        if (buf2.readableByteCount() != 14) {
            throw new RuntimeException();
        }
    }

    @Test
    public void advance() {
        DefaultDataBufferFactory fac = new DefaultDataBufferFactory();
        DataBuffer buf1 = fac.allocateBuffer(1024);
        buf1.write(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
        if (buf1.readableByteCount() != 16) {
            throw new RuntimeException();
        }
        buf1.readPosition(buf1.readPosition() + 6);
        if (buf1.readableByteCount() != 10) {
            throw new RuntimeException();
        }
        DataBuffer buf2 = buf1.slice(3, 4);
        if (buf2.readPosition() != 0) {
            throw new RuntimeException();
        }
        if (buf2.readableByteCount() != 4) {
            throw new RuntimeException();
        }
        if (buf2.getByte(0) != 3) {
            throw new RuntimeException();
        }
        if (buf2.getByte(0) != 3) {
            throw new RuntimeException();
        }
        if (buf2.read() != 3) {
            throw new RuntimeException();
        }
        if (buf2.read() != 4) {
            throw new RuntimeException();
        }
    }

    @Test
    public void advance2() {
        DefaultDataBufferFactory fac = new DefaultDataBufferFactory();
        DataBuffer buf1 = fac.allocateBuffer(1024);
        buf1.write(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
        DataBuffer buf2 = buf1.slice(0, buf1.readableByteCount());
        buf1.readPosition(buf1.readPosition() + 4);
        if (buf2.readableByteCount() != 16) {
            throw new RuntimeException();
        }
        if (buf2.getByte(0) != 0) {
            throw new RuntimeException();
        }
        if (buf2.read() != 0) {
            throw new RuntimeException();
        }
        if (buf1.read() != 4) {
            throw new RuntimeException();
        }
        if (buf2.read() != 1) {
            throw new RuntimeException();
        }
        if (buf1.read() != 5) {
            throw new RuntimeException();
        }
    }

    @Test
    public void convertRepositionRead() {
        DefaultDataBufferFactory fac = new DefaultDataBufferFactory();
        DataBuffer buf1 = fac.allocateBuffer(1024);
        buf1.write(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
        ByteBuffer buf2 = buf1.asByteBuffer();
        buf1.readPosition(8);
        if (buf2.position() != 0) {
            throw new RuntimeException();
        }
        if (buf2.remaining() != 16) {
            throw new RuntimeException();
        }
        if (buf2.get() != 0) {
            throw new RuntimeException();
        }
    }

    @Test
    public void repositionConvertRead() {
        DefaultDataBufferFactory fac = new DefaultDataBufferFactory();
        DataBuffer buf1 = fac.allocateBuffer(1024);
        buf1.write(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
        buf1.readPosition(2);
        ByteBuffer buf2 = buf1.asByteBuffer();
        if (buf2.position() != 0) {
            throw new RuntimeException();
        }
        if (buf2.remaining() != 14) {
            throw new RuntimeException();
        }
        if (buf2.get() != 2) {
            throw new RuntimeException();
        }
    }

    @Test
    public void copy() {
        DefaultDataBufferFactory fac = new DefaultDataBufferFactory();
        DataBuffer buf1 = fac.allocateBuffer(1024);
        buf1.write(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
        if (buf1.writePosition() != 16) {
            throw new RuntimeException();
        }
        buf1.readPosition(3);
        buf1.writePosition(7);
        DataBuffer buf2 = buf1.factory().allocateBuffer(128);
        buf2.write(buf1);
        if (buf1.readableByteCount() != 4) {
            throw new RuntimeException();
        }
        if (buf2.readableByteCount() != 4) {
            throw new RuntimeException();
        }
        if (buf2.readPosition() != 0) {
            throw new RuntimeException();
        }
        if (buf2.writePosition() != 4) {
            throw new RuntimeException();
        }
    }

    @Test
    public void byteBuffer() {
        assertEquals(-1, (byte)0xff);
        ByteBuffer buf = ByteBuffer.allocate(128);
        buf.put(0, (byte)0xff);
        assertEquals("ff000000", BaseEncoding.base16().lowerCase().encode(buf.array(), 0, 4));
        assertEquals("ffffffff", String.format("%2x", -1));
        assertEquals("ffff", String.format("%2x", (short)-1));
        assertEquals("ff", String.format("%2x", (byte)-1));
        assertEquals("ff", String.format("%2x", buf.get(0)));
        assertEquals(-1, buf.get(0));
        assertEquals(255, 0xff & buf.get(0));
        assertEquals(-1, ((byte)0xff) & buf.get(0));
        short a = (short) (0xff & buf.get(0));
        assertEquals(255, a);
    }

}
