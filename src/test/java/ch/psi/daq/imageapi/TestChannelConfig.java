package ch.psi.daq.imageapi;

import org.junit.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
Test for the currently used config file format.
No formal specs, therefore reverse engineered from reader source code and data on disk.
*/
public class TestChannelConfig {

    static void createTestConfigEmpty() throws IOException {
        Path path = Path.of("test", "data");
        Files.createDirectories(path);
        Path path2 = path.resolve("config2");
        SeekableByteChannel wr = Files.newByteChannel(path2, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING));
        ByteBuffer buf = ByteBuffer.allocate(1024);
        buf.putShort((short) 0);
        ByteBuffer buf2 = StandardCharsets.UTF_8.encode("ChannelConfig2");
        int channelNameLen = buf2.remaining() + 2 * Integer.BYTES;
        buf.putInt(channelNameLen);
        buf.put(buf2);
        buf.putInt(channelNameLen);
        buf.flip();
        wr.write(buf);
        wr.close();
    }

    public static void createTestConfigEmpty(Path path, String name) throws IOException {
        Files.createDirectories(path.getParent());
        SeekableByteChannel wr = Files.newByteChannel(path, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING));
        ByteBuffer buf = ByteBuffer.allocate(1024);
        buf.putShort((short) 0);
        ByteBuffer buf2 = StandardCharsets.UTF_8.encode(name);
        int channelNameLen = buf2.remaining() + 2 * Integer.BYTES;
        buf.putInt(channelNameLen);
        buf.put(buf2);
        buf.putInt(channelNameLen);
        buf.flip();
        wr.write(buf);
        wr.close();
    }

    static void createTestConfig7() throws IOException {
        Path path = Path.of("test", "data");
        Files.createDirectories(path);
        Path path2 = path.resolve("config3");
        SeekableByteChannel wr = Files.newByteChannel(path2, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING));
        ByteBuffer buf = ByteBuffer.allocate(1024 * 1024);
        buf.putShort((short) 0);
        ByteBuffer buf2 = StandardCharsets.UTF_8.encode("ChannelConfig3");
        int channelNameLen = buf2.remaining() + 2 * Integer.BYTES;
        buf.putInt(channelNameLen);
        buf.put(buf2);
        buf.putInt(channelNameLen);

        class Meta {
            byte compression;
            long beg;
        }
        List<Meta> metas = new ArrayList<>();
        for (int i1 = 0; i1 < 7; i1 += 1) {
            Meta m = new Meta();
            m.compression = (byte) (((i1 + 1) % 4) - 1);
            m.beg = (7 + i1) * 3600_000_000_000L;
            metas.add(m);
        }

        for (Meta meta : metas) {
            // TODO
            int metaLenPos = buf.position();
            buf.putInt(0);

            long unscaledTime = meta.beg;
            long pulse = 4242;
            int keyspace = 3;
            long binSize = 3600_000_000_000L;
            int splitCount = 1;
            int status = -1;
            byte backendByte = 1;
            int modulo = 1;
            int offset = 12;
            short precision = 5252;
            buf.putLong(unscaledTime);
            buf.putLong(pulse);
            buf.putInt(keyspace);
            buf.putLong(binSize);
            buf.putInt(splitCount);
            buf.putInt(status);
            buf.put(backendByte);
            buf.putInt(modulo);
            buf.putInt(offset);
            buf.putShort(precision);

            // TODO
            //int typeLenPos = buf.position();
            buf.putInt(2);

            if (meta.compression >= 0) {
                buf.putShort((short)0x8000);
                buf.put(meta.compression);
            }
            else {
                buf.putShort((short)0x0000);
            }

            buf.putInt(0);
            buf.putInt(0);
            buf.putInt(0);
            buf.putInt(0);
            buf.putInt(0);

            int n;
            n = buf.position() - metaLenPos + Integer.BYTES;
            buf.putInt(metaLenPos, n);
            buf.putInt(n);
        }
        buf.flip();
        wr.write(buf);
        wr.close();
    }

    @Test
    public void emptyConfig() throws IOException {
        createTestConfigEmpty();
        List<ChannelConfig> l1 = ChannelConfigFileReader.read(Path.of("test/data/config2"), Instant.MIN, Instant.MAX);
        assertEquals(0, l1.size());
    }

    @Test
    public void sevenConfig() throws IOException {
        createTestConfig7();
        List<ChannelConfig> l1 = ChannelConfigFileReader.read(Path.of("test/data/config3"), Instant.ofEpochSecond(8 * 3600), Instant.ofEpochSecond(10 * 3600));
        assertEquals(3, l1.size());
        List<ChannelConfig> l2 = ChannelConfigFileReader.read(Path.of("test/data/config3"), null, null);
        assertEquals(7, l2.size());
    }

    @Test
    public void testChannelConfig() throws IOException {
        List<ChannelConfig> l1 = ChannelConfigFileReader.read(Path.of("test/data/config1"), Instant.MIN, Instant.MAX);
        assertEquals(9, l1.size());
    }

    @Test
    public void constructor() {
        new ChannelConfigFileReader();
    }

}
