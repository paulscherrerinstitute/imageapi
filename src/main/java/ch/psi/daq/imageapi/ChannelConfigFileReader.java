package ch.psi.daq.imageapi;

import ch.psi.bsread.compression.Compression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ChannelConfigFileReader {

    private static final Logger logger = LoggerFactory.getLogger(ChannelConfigFileReader.class);

    public static List<ChannelConfig> read(Path path, Instant start, Instant end) throws IOException {

        logger.info("Reading config file: {}", path);
//        byte[] allBytes = Files.readAllBytes(path); // we read in the whole file (it needs to fit in memory anyways)
//        ByteBuffer b = ByteBuffer.wrap(allBytes);

        List<ChannelConfig> configurations = new ArrayList<>();

        try(FileChannel fc = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate((int) fc.size()); // we read the whole file
            buffer.order(ByteOrder.BIG_ENDIAN);
            fc.read(buffer);
            buffer.flip(); // make buffer ready for read

            // Get version of serializer
            short version = buffer.getShort();

            // Get channel name
            int length = buffer.getInt();
            byte[] s = new byte[length-Integer.BYTES-Integer.BYTES]; // check length is withing length
            buffer.get(s);
            String channelName = new String(s);
            int lengthCheck = buffer.getInt();

            // Get metadata blob
            while(buffer.remaining()>0) {

                // Remember start position as we need this to skip the end of the metadata block (as it is filled by FF right now)
                int startPosition = buffer.position();
                int metaLength = buffer.getInt();

                long unscaledTime = buffer.getLong();

                Instant timestamp = Instant.ofEpochSecond(
                unscaledTime/1_000_000_000L,
                unscaledTime % 1_000_000_000L);

                if((start!=null && timestamp.isBefore(start)) || (end != null && timestamp.isAfter(end))){
                    // if time is not within the specified timerange skip to the next entry
                    buffer.position(startPosition + metaLength);
                    continue;
                }

                long pulseId = buffer.getLong();
                int keyspace = buffer.getInt();
                long binSize = buffer.getLong();
                int splitCount = buffer.getInt();
                int status = buffer.getInt();
                byte backendByte = buffer.get();
                int modulo = buffer.getInt();
                int offset = buffer.getInt();
                short precision = buffer.getShort();

                // Get dtype - use DataBitmaskUtils to make sense of the bitmask
                DType dtype;
                int typeInfoSize = buffer.getInt();
                byte[] b = new byte[typeInfoSize];
                buffer.get(b);
                short bitmap = ByteBuffer.wrap(b).getShort();

                dtype = new DType();
                dtype.setType(DTypeBitmapUtils.getType(bitmap));
                dtype.setByteOrder(DTypeBitmapUtils.getByteOrder(bitmap));
                Compression compression = Compression.none;
                if(DTypeBitmapUtils.isCompressed(bitmap)){
                    byte compressionNumber = buffer.get();
                    if(compressionNumber == 0 ) {
                        compression = Compression.bitshuffle_lz4;
                    }
                    else if(compressionNumber == 1) {
                        compression = Compression.lz4;
                    }
                }
                dtype.setCompression(compression);
//                if(DTypeBitmapUtils.hasShape(bitmap)){
//                    short numberOfDimensions = buffer.getShort();
//                    int[] shape = new int[numberOfDimensions];
//                    for(int i = 0; i<numberOfDimensions; i++){
//                        shape[i] = buffer.getInt();
//                    }
//                    dtype.setShape(shape);
//                }
//                else{
                dtype.setShape(null);
//                }

                // Read source
                String source = readLenghtAndString(buffer);

                // all these properties are currently empty (i.e. null)
                String unit = readLenghtAndString(buffer);
                String description = readLenghtAndString(buffer);
                String optionalFields = readLenghtAndString(buffer); // usually encoded as json
                String valueConverter = readLenghtAndString(buffer);

                lengthCheck = buffer.getInt(); // end length of the meta blob

                // TODO Check length, etc.

                // Skip the rest of the blob / make sure that we are at the right position for the next blob
                // no matter what happened within this blob (if everything went ok this is not needed!)
//                buffer.position(startPosition + metaLength);

                ChannelConfig config = new ChannelConfig(channelName, timestamp, pulseId, keyspace, binSize, splitCount, status, backendByte, modulo, offset, precision, dtype, source, unit, description, optionalFields, valueConverter);
                configurations.add(config);

            }

        } // closes the file channel

        return configurations;
    }

    /**
     * Read strings that are prepended with its length. It will increase the ByteBuffer position accordingly
     * &lt;length>&lt;string>
     * @param buffer
     * @return
     */
    private static String readLenghtAndString(ByteBuffer buffer){
        int length = buffer.getInt();
        if(length <= 0){
            return null;
        }
        byte[] value = new byte[length - Integer.BYTES];
        buffer.get(value);
        return new String(value);
    }
}
