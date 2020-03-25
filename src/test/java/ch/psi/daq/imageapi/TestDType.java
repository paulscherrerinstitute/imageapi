package ch.psi.daq.imageapi;

import org.junit.Test;

import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.*;

public class TestDType {

    @Test
    public void typeFlagExtractors() {
        // Just exercise the accessor code paths...
        assertEquals(DTypeBitmapUtils.Type.FLOAT32, DTypeBitmapUtils.getType((short) 11));
        assertTrue(DTypeBitmapUtils.isCompressed((short) 0x8000));
        assertTrue(DTypeBitmapUtils.isArray((short) 0x4000));
        assertEquals(ByteOrder.BIG_ENDIAN, DTypeBitmapUtils.getByteOrder((short) 0x2000));
        assertTrue(DTypeBitmapUtils.hasShape((short) 0x1000));
        assertEquals((short) 0x900c, DTypeBitmapUtils.calculateBitmask(DTypeBitmapUtils.Type.FLOAT64, ByteOrder.LITTLE_ENDIAN, false, true, true));
    }

}
