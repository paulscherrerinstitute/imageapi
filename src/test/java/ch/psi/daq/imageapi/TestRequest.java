package ch.psi.daq.imageapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureWebTestClient
public class TestRequest {
    static final Logger LOGGER = LoggerFactory.getLogger(TestRequest.class);

    @Value("${imageapi.dataBaseDir:UNDEF}")
    String dataBaseDir;

    @Value("${imageapi.baseKeyspaceName:UNDEF}")
    String baseKeyspaceName;

    @Autowired
    private WebTestClient client;

    @BeforeClass
    public static void beforeClass() {
    }

    /*
    Write test data in the version 2016 data format.
    */
    void writeChunk(SeekableByteChannel wrData, SeekableByteChannel wrIndex, ByteBuffer buf, long ts, long pulse) throws IOException {
        Random rng = new Random(pulse);
        int valueLen = 22 + 0 * ((rng.nextInt() >> 14) & 0xfffffffc);
        rng.setSeed(pulse);
        long ttl = 42;
        long iocTime = ts;
        byte status = 0;
        byte severity = 0;
        int optionalFieldsLength = -1;
        short typeBits = (short) (5 | 0x8000 | 0x1000);
        byte compressionMethod = 0;
        byte shapeDim = 2;
        int[] shapeExt = {1024, 512};
        int totalLen = 2 * Integer.BYTES + 4 * Long.BYTES + 2 * Byte.BYTES +
          Integer.BYTES + Short.BYTES + 2 * Byte.BYTES + shapeDim * Integer.BYTES + valueLen;
        buf.putInt(totalLen);
        buf.putLong(ttl);
        buf.putLong(ts);
        buf.putLong(pulse);
        buf.putLong(iocTime);
        buf.put(status);
        buf.put(severity);
        buf.putInt(optionalFieldsLength);
        buf.putShort(typeBits);
        buf.put(compressionMethod);
        buf.put(shapeDim);
        buf.putInt(shapeExt[0]);
        buf.putInt(shapeExt[1]);
        int i2 = valueLen;
        while (i2 > 0) {
            buf.put((byte)(rng.nextInt() & 0x7f));
            i2 -= 1;
        }
        buf.putInt(totalLen);
        buf.flip();
        long pos = wrData.position();
        wrData.write(buf);
        buf.clear();

        buf.putLong(ts);
        buf.putLong(pos);
        buf.flip();
        wrIndex.write(buf);
        buf.clear();
    }

    void writeData(SeekableByteChannel wrData, SeekableByteChannel wrIndex, String name, int bin, int split) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(2<<22);
        buf.putShort((short)0);
        ByteBuffer nameEnc = StandardCharsets.UTF_8.encode(name);
        int channelNameLen = 2 * Integer.BYTES + nameEnc.remaining();
        buf.putInt(channelNameLen);
        buf.put(nameEnc);
        buf.putInt(channelNameLen);
        buf.flip();
        wrData.write(buf);

        buf.clear();
        buf.putShort((short)0);
        buf.flip();
        wrIndex.write(buf);

        long binL = (long) bin;
        long tsBeg = (binL + 0) * 3600 * 1000 * 1000 * 1000 + 0x010203;
        long tsEnd = (binL + 1) * 3600 * 1000 * 1000 * 1000;

        buf.clear();
        for (long ts = tsBeg; ts < tsEnd; ts += 2L * 60 * 1000 * 1000 * 1000) {
            writeChunk(wrData, wrIndex, buf, ts, ts + 13);
        }
    }

    void createSplit(Path path, String name, int bin, int split) throws IOException {
        Path path2 = path.resolve(String.format("%010d", split));
        Path path3 = path2.resolve(String.format("%019d_%05d_Data", 3600 * 1000, 0));
        Path path4 = path2.resolve(String.format("%019d_%05d_Data_Index", 3600 * 1000, 0));
        Files.createDirectories(path2);
        SeekableByteChannel wrData = Files.newByteChannel(path3, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING));
        SeekableByteChannel wrIndex = Files.newByteChannel(path4, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING));
        writeData(wrData, wrIndex, name, bin, split);
        wrData.close();
        wrIndex.close();
    }

    void createBin(Path path, String name, int bin) throws IOException {
        Path path2 = path.resolve(String.format("%019d", bin));
        Files.createDirectories(path2);
        for (int i1 = 0; i1 < 4; i1++) {
            createSplit(path2, name, bin, i1);
        }
    }

    void createChannel(Path path, String name) throws IOException {
        Path path2 = path.resolve(name);
        Files.createDirectories(path2);
        createBin(path2, name, 440000);
        createBin(path2, name, 440001);
        createBin(path2, name, 440002);
        createBin(path2, name, 440003);
    }

    void setupData() throws IOException {
        // /gpfs/sf-data/sf-imagebuffer/daq_swissfel/daq_swissfel_4/byTime/SARES20-CAMS142-M5:FPICTURE/0000000000000439983/0000000002/0000000000003600000_00000_Data
        Path path = Path.of(dataBaseDir).resolve(baseKeyspaceName).resolve(baseKeyspaceName + "_4").resolve("byTime");
        createChannel(path, "chn002");
        createChannel(path, "chn003");
    }

    @Test
    public void contactHost() throws IOException {
        setupData();
        client.get().uri("/api/v1/q1")
        .exchange()
        .expectStatus().isOk()
        .expectBody(DataBuffer.class)
        .value(x -> {
            assertNotNull(x);
            assertEquals(String.format("Hi there %s", dataBaseDir), x.toString(StandardCharsets.UTF_8));
        });
    }

    @Test
    public void searchChannel() throws IOException {
        setupData();
        Set<String> expect = Set.of("chn002", "chn003");
        client.get().uri("/api/v1/channels?regex=.*")
        .exchange()
        .expectStatus().isOk()
        .expectBody(DataBuffer.class)
        .value(x -> {
            assertNotNull(x);
            String s = x.toString(StandardCharsets.UTF_8);
            ObjectReader or = new ObjectMapper().readerFor(String[].class);
            try {
                List<String> l1 = Arrays.asList(or.readValue(s));
                Set<String> set1 = Set.copyOf(l1);
                assertEquals(expect, set1);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void queryData() throws IOException {
        // TODO do only once
        setupData();
        client.post().uri("/api/v1/query")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{\"channels\":[\"chn002\"], \"range\": {\"type\":\"date\", \"startDate\":\"2020-03-12T09:10:00Z\", \"endDate\":\"2020-03-12T09:20:00Z\"}}")
        .exchange()
        .expectStatus().isOk()
        .expectBody(DataBuffer.class)
        .value(x -> {
            assertNotNull(x);
            assertTrue(500 < x.readableByteCount());
            assertTrue(20000 > x.readableByteCount());
        });
    }

}
