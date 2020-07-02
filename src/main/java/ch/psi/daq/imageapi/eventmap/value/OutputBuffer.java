package ch.psi.daq.imageapi.eventmap.value;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class OutputBuffer extends OutputStream {
    DataBufferFactory bufFac;
    DataBuffer buf;
    int bufferSize = 8 * 1024;
    List<DataBuffer> pendingBuffers = new ArrayList<>();

    public OutputBuffer(DataBufferFactory bufFac) {
        this.bufFac = bufFac;
    }

    @Override
    public void write(int val) {
        if (buf == null) {
            buf = bufFac.allocateBuffer(bufferSize);
        }
        if (buf.writableByteCount() < 1) {
            pendingBuffers.add(buf);
            buf = bufFac.allocateBuffer(bufferSize);
        }
        buf.write((byte) val);
    }

    @Override
    public void close() {
        pendingBuffers.add(buf);
        buf = null;
    }

    public void release() {
        if (buf != null) {
            DataBufferUtils.release(buf);
            buf = null;
        }
        if (pendingBuffers != null) {
            for (DataBuffer buf : pendingBuffers) {
                DataBufferUtils.release(buf);
            }
            pendingBuffers = null;
        }
    }

    public List<DataBuffer> getPending() {
        List<DataBuffer> ret = pendingBuffers;
        pendingBuffers = new ArrayList<>();
        return ret;
    }

}
