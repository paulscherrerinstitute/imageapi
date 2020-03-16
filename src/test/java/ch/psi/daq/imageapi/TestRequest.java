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

    @Autowired
    private Controller ctrl;

    @BeforeClass
    public static void beforeClass() {
    }

    class TimestampGenerator {
        Random rng;
        long ts;
        long tsEnd;
        int off;
        TimestampGenerator(long ts, long tsEnd) {
            this.ts = ts;
            this.tsEnd = tsEnd;
            rng = new Random(ts);
            this.off = rng.nextInt(0xfff);
        }
        public void advance() {
            ts += 20L * 1000 * 1000 * 1000;
            this.off = rng.nextInt(0xffff);
        }
        public int bin() {
            return (int) (ts() / (3600L * 1000 * 1000 * 1000));
        }
        public long ts() {
            return ts + off;
        }
    }

    /*
    Write test data in the version 2016 data format.
    */
    void writeChunk(SeekableByteChannel wrData, SeekableByteChannel wrIndex, ByteBuffer buf, long ts, long pulse) throws IOException {
        Random rng = new Random(pulse);
        int valueLen = 400 + rng.nextInt(300);
        rng.setSeed(pulse);
        long ttl = 42;
        long iocTime = ts + 1;
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

    void writeData(SeekableByteChannel wrData, SeekableByteChannel wrIndex, String name, int bin, int split, TimestampGenerator tsgen) throws IOException {
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

        buf.clear();
        while (tsgen.bin() == bin) {
            writeChunk(wrData, wrIndex, buf, tsgen.ts(), tsgen.ts() + 13);
            tsgen.advance();
        }
    }

    void createSplit(Path path, String name, int bin, int split, TimestampGenerator tsgen) throws IOException {
        Path path2 = path.resolve(String.format("%010d", split));
        Path path3 = path2.resolve(String.format("%019d_%05d_Data", 3600 * 1000, 0));
        Path path4 = path2.resolve(String.format("%019d_%05d_Data_Index", 3600 * 1000, 0));
        Files.createDirectories(path2);
        SeekableByteChannel wrData = Files.newByteChannel(path3, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING));
        SeekableByteChannel wrIndex = Files.newByteChannel(path4, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING));
        writeData(wrData, wrIndex, name, bin, split, tsgen);
        wrData.close();
        wrIndex.close();
    }

    void createBin(Path path, String name, int bin) throws IOException {
        Path path2 = path.resolve(String.format("%019d", bin));
        Files.createDirectories(path2);
        for (int split = 0; split < 4; split++) {
            TimestampGenerator tsgen = new TimestampGenerator(((long)bin) * 3600 * 1000 * 1000 * 1000 + (split << 16), ((long)bin+1) * 3600 * 1000 * 1000 * 1000);
            createSplit(path2, name, bin, split, tsgen);
        }
    }

    void createChannel(Path path, String name) throws IOException {
        Path path2 = path.resolve(name);
        Files.createDirectories(path2);
        for (int bin = 440000; bin < 440004; bin += 1) {
            createBin(path2, name, bin);
        }
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

    void parseQueryResponse(ByteBuffer bb) {
        int len0 = bb.getInt(0);
        assertTrue(1024 > len0);
        int pos = 2 * Integer.BYTES + len0;
        while (pos < bb.limit()) {
            int len = bb.getInt(pos);
            assertTrue(20000 > len);
            assertEquals(1, bb.get(pos + Integer.BYTES));
            long ts = bb.getLong(pos + Integer.BYTES + Byte.BYTES);
            long pulse = bb.getLong(pos + Integer.BYTES + Byte.BYTES + Long.BYTES);
            long bin = ts / (3600L * 1000 * 1000 * 1000);
            assertTrue(440001 <= bin);
            assertTrue(440002 > bin);
            assertTrue((440001L * 3600 + 10 * 60) * 1000 * 1000 * 1000 <= ts);
            assertTrue((440001L * 3600 + 20 * 60) * 1000 * 1000 * 1000 >  ts);
            pos += 2 * Integer.BYTES + len;
        }
    }

    @Test
    public void queryData() throws IOException {
        // TODO do only once
        setupData();
        ctrl.bufferSize = 17;
        client.post().uri("/api/v1/query")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{\"channels\":[\"chn002\"], \"range\": {\"type\":\"date\", \"startDate\":\"2020-03-12T09:10:00Z\", \"endDate\":\"2020-03-12T09:20:00Z\"}}")
        .exchange()
        .expectStatus().isOk()
        .expectBody(ByteBuffer.class)
        .value(x -> {
            assertNotNull(x);
            parseQueryResponse(x);
        });
    }

}
