/**
 * Copied from ch.psi.daq.databuffer project
 *
 */

package ch.psi.daq.imageapi;

import ch.psi.bsread.compression.Compression;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.nio.ByteOrder;

public class DType {
    private DTypeBitmapUtils.Type type;
    // TODO need custom serializer for both
    @JsonIgnore
    private ByteOrder byteOrder;
    private Compression compression;
    private int[] shape;

    // UTILITY PROPERTY (kind of a hack) - position of the blob bytebuffer to read the value from
    @JsonIgnore
    private int bufferReadOffsetForValue;

    public DTypeBitmapUtils.Type getType() {
        return type;
    }

    public void setType(DTypeBitmapUtils.Type type) {
        this.type = type;
    }

    public ByteOrder getByteOrder() {
        return byteOrder;
    }

    public void setByteOrder(ByteOrder byteOrder) {
        this.byteOrder = byteOrder;
    }

    public Compression getCompression() {
        return compression;
    }

    public void setCompression(Compression compression) {
        this.compression = compression;
    }

    public int[] getShape() {
        return shape;
    }

    public void setShape(int[] shape) {
        this.shape = shape;
    }


    public int getBufferReadOffsetForValue() {
        return bufferReadOffsetForValue;
    }

    public void setBufferReadOffsetForValue(int bufferReadOffsetForValue) {
        this.bufferReadOffsetForValue = bufferReadOffsetForValue;
    }
}
