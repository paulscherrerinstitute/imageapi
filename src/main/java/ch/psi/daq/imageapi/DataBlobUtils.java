package ch.psi.daq.imageapi;

import ch.psi.bsread.compression.Compression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

// TODO functions in here need to be bundled/encapsulated with the buffer as they operate on/with the position of the buffer !!!!

/**
 *
 * IMPORTANT !!!! NONE of the methods in this class must increase/decrease the buffers current read position
 *
 * A Data blob format looks as follows.
 * (the buffer passed to these routines does not include the length in front and at the end of the blob)
 *
 * [long - time to live in ms]
 * [long - (global) timestamp in ms]
 * [long - pulseId]
 * [long - iocTime in ns
 * [byte - status]
 * [byte - severity]
 * [int - size optional fields]
 *      [byte[size] - String optional fields as JSON]
 * [short - dtype bitmask - bitmask containing information for: type, byteorder, compression, is-array, shape ]
 *      [byte] - if compression bit is set - compression algorithm
 *
 *      [short - number of dimensions] - if shape bit is set
 *      [int[number-of-dimensions]] - shape sizes
 * [bytes... - value]
 */
public class DataBlobUtils {

    private static final Logger logger = LoggerFactory.getLogger(DataBlobUtils.class);

    public static int POSITION_TTL = 0;
    public static int POSITION_GLOBAL_TIMESTAMP = POSITION_TTL + Long.BYTES;
    public static int POSITION_PULSE_ID = POSITION_GLOBAL_TIMESTAMP + Long.BYTES;
    public static int POSITION_IOC_TIMESTAMP = POSITION_PULSE_ID + Long.BYTES;
    public static int POSITION_STATUS = POSITION_IOC_TIMESTAMP + Long.BYTES;
    public static int POSITION_SEVERITY = POSITION_STATUS + Byte.BYTES;
    public static int POSITION_OPTIONAL_FIELDS = POSITION_SEVERITY + Byte.BYTES;
    public static int POSITION_DTYPE = POSITION_OPTIONAL_FIELDS + Integer.BYTES; // + SIZE_OPTIONAL_FIELDS // Important - we also have to add the size of the optional fields if size was >0
    // ALL OTHER READING POSITIONS NEED TO BE CALCULATED (its more complicated than with optional fields)
    // BASED ON THE CONTENT OF THE DYTPE BITMAP

    // Utility functions to only extract certain fields out of the value blob

    /**
     * Get TTL
     * @return  ttl in ms
     */
    public static long getTTL(ByteBuffer buffer){
        return buffer.getLong(buffer.position() + POSITION_TTL);
    }

    /**
     * Get global timestamp
     * @param buffer
     * @return (global) timestamp in ns
     */
    public static long getGlobalTimestamp(ByteBuffer buffer){
        return buffer.getLong(buffer.position() + POSITION_GLOBAL_TIMESTAMP);
    }

    /**
     * Get pulse id
     * @param buffer
     * @return
     */
    public static long getPulseId(ByteBuffer buffer){
        return buffer.getLong(buffer.position() + POSITION_PULSE_ID);
    }

    /**
     * Get ioc timestamp
     * @param buffer
     * @return (ioc) timestamp in ns
     */
    public static long getIOCTimestamp(ByteBuffer buffer){
        return buffer.getLong(buffer.position() + POSITION_IOC_TIMESTAMP);
    }

    public static byte getStatus(ByteBuffer buffer){
        return buffer.get(buffer.position() + POSITION_STATUS);
    }

    public static byte getSeverity(ByteBuffer buffer){
        return buffer.get(buffer.position() + POSITION_SEVERITY);
    }

    /**
     * Get optional fields
     * @param buffer
     * @return  JSON encoded string of optional fields, null if no optional fields are available
     */
    public static String getOptionalFields(ByteBuffer buffer){
        // Optional fields - I guess this bit of information does not belong into this blob. This makes parsing extremely cumbersome
        // as all following readings are based on this field - remove in future versions !!!!
        int size = buffer.getInt(buffer.position() +POSITION_OPTIONAL_FIELDS);
        if(size != -1){
            logger.info("Optional fields included!");
            byte[] s = new byte[size];
            buffer.get(s);
            String optionalFields = new String(s); // JSON encoded optional fields
            return optionalFields;
        }
        return null;
    }

    public static short getDTypeBitmap(ByteBuffer buffer){
        return buffer.getShort(buffer.position() + POSITION_DTYPE + getOptionalFieldsSize(buffer));
    }

    public static DType getDType(ByteBuffer buffer){
        short bitmap = getDTypeBitmap(buffer);

        DType dtype = new DType();
        dtype.setType(DTypeBitmapUtils.getType(bitmap));
        dtype.setByteOrder(DTypeBitmapUtils.getByteOrder(bitmap));

        int bufferReadPosition = buffer.position() + POSITION_DTYPE + getOptionalFieldsSize(buffer) + Short.BYTES;
        Compression compression = Compression.none;
        if(DTypeBitmapUtils.isCompressed(bitmap)){
            byte compressionNumber = buffer.get(bufferReadPosition);
            bufferReadPosition += Byte.BYTES; // Update buffer read position for next read

            if(compressionNumber == 0 ) {
                compression = Compression.bitshuffle_lz4;
            }
            else if(compressionNumber == 1) {
                compression = Compression.lz4;
            }
        }
        dtype.setCompression(compression);

        // DTypeBitmapUtils.isArray(dtype); // Somehow this is redundant to shape

        if(DTypeBitmapUtils.hasShape(bitmap)){
            short numberOfDimensions = (short) (buffer.get(bufferReadPosition) & 0xff);
            bufferReadPosition += Byte.BYTES;

            int[] shape = new int[numberOfDimensions];
            for(int i = 0; i<numberOfDimensions; i++){
                shape[i] = buffer.getInt(bufferReadPosition);
                bufferReadPosition += Integer.BYTES;
            }
            dtype.setShape(shape);
        }
        else{
            dtype.setShape(null);
        }


        // bufferReadPosition now holds the position to read the bytes for the actual value
        // (this is kind of a hack)
        dtype.setBufferReadOffsetForValue(bufferReadPosition);

        return dtype;
    }

    public static byte[] getValueBytes(ByteBuffer buffer){
        DType dType = getDType(buffer);
        return getValueBytes(buffer, dType.getBufferReadOffsetForValue());
    }

    public static byte[] getValueBytes(ByteBuffer buffer, int bufferReadOffset){
        int originalPosition = buffer.position();
        int length = buffer.limit()-bufferReadOffset;
        byte[] value = new byte[length];
        buffer.position(bufferReadOffset);
        buffer.get(value);
        buffer.position(originalPosition);
        return value;
    }


    // Utility function to get the size of the optional field blob - its a hack due to the suboptimal implementation of the blob format
    private static int getOptionalFieldsSize(ByteBuffer buffer){
        int sizeOptionalFields = buffer.getInt(buffer.position() +POSITION_OPTIONAL_FIELDS);
        if(sizeOptionalFields < 0){
            sizeOptionalFields = 0;
        }
        return sizeOptionalFields;
    }

//    public static <T> T getValue(ByteBuffer buffer, DType dtype){
//        switch (dtype.getType()){
//            case BOOL:
//                break;
//            case BOOL8:
//                break;
//            case INT8:
//                break;
//            case UINT8:
//                break;
//            case INT16:
//                break;
//            case UINT16:
//                break;
//            case CHARACTER:
//                break;
//            case INT32:
//                break;
//            case UINT32:
//                break;
//            case INT64:
//                break;
//            case UINT64:
//                break;
//            case FLOAT32:
//                break;
//            case FLOAT64:
//                buffer.asDoubleBuffer().get();
//                break;
//            case STRING:
//                break;
//        }
//        return null;
//    }


//    /**
//     * Deserialize data
//     * @param buffer
//     * @param debug - for test purposes only
//     * @return
//     */
//    public static boolean deserializeValueBlob(ByteBuffer buffer, boolean debug){
//        long ttlTime = buffer.getLong(); // in ms
//        long globalTime = buffer.getLong(); // in ns
//        long pulseId = buffer.getLong();
//        long iocTime = buffer.getLong(); // in ns
//        byte status = buffer.get();
//        byte severity = buffer.get();
//
//        // Optional fields - I guess this bit of information does not belong into this file
//        int size = buffer.getInt();
//        if(size != -1){
//            logger.info("Optional fields included!");
//            byte[] s = new byte[size];
//            buffer.get(s);
//            String optionalFields = new String(s); // JSON encoded
//        }
//
//        // All this should be cached based on whats coming with dtype
//        short dtype = buffer.getShort();
//        // type, byteorder, compression, isarray, shape
////                DataBitmaskUtils.getType(dtype);
////                DataBitmaskUtils.getByteOrder(dtype);  // we ignore the byte order so far
////                DataBitmaskUtils.isCompressed(dtype);
////                DataBitmaskUtils.isArray(dtype);
////                DataBitmaskUtils.hasShape(dtype);
//
//        Compression compression = Compression.none;
//        if(DTypeBitmapUtils.isCompressed(dtype)){
//            byte compressionNumber = buffer.get();
//
//            if(compressionNumber == 0 ) {
//                compression = Compression.bitshuffle_lz4;
//            }
//            else if(compressionNumber == 1) {
//                compression = Compression.lz4;
//            }
//        }
//
//        if(DTypeBitmapUtils.hasShape(dtype)){
//            short numberOfDimensions = buffer.get();
//            int[] shape = new int[numberOfDimensions];
//            for(int i = 0; i<numberOfDimensions; i++){
//                shape[i] = buffer.getInt();
//            }
//        }
//
//        // TODO read value (depending on type)
//        // TODO need to decompress blob if needed ...
//
//        long numberOfBytesData = buffer.limit() - buffer.position(); // TODO this is not correct if were not at the end of the file !!!!!
//
//        double value = 0;
//        // interpret value(s) correctly
//        switch(DTypeBitmapUtils.getType(dtype)){
//            case FLOAT64:
//                // TODO if shape use asDoubleBuffer().get()...
//                value = buffer.getDouble();
//                break;
//
//            // TODO need to implement all cases
//        }
//        // <<<< END DESERIALIZE DATA
//
//        if(debug){
//            logger.info("{} {} {} {} {} {} {} {} {} {} {}", Instant.ofEpochSecond(globalTime/1_000_000_000L, globalTime % 1_000_000_000L), ttlTime, globalTime, pulseId, iocTime, status, severity, size, dtype, numberOfBytesData, value);
//        }
//
//        return true;
//    }
}
