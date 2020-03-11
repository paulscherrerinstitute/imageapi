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
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

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

    void createSplit(Path path, String name, int bin, int split) throws IOException {
        Path path2 = path.resolve(String.format("%010d", split));
        Path path3 = path2.resolve(String.format("%019d_%05d_Data", 3600 * 1000, 0));
        Files.createDirectories(path2);
        BufferedWriter wr = Files.newBufferedWriter(path3);
        wr.write(47);
        wr.write(48);
        wr.write(10);
        wr.close();
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
        createBin(path2, name, 430100);
        createBin(path2, name, 430101);
        createBin(path2, name, 430102);
    }

    void setupData() throws IOException {
        // /gpfs/sf-data/sf-imagebuffer/daq_swissfel/daq_swissfel_4/byTime/SARES20-CAMS142-M5:FPICTURE/0000000000000439983/0000000002/0000000000003600000_00000_Data
        Path path = Path.of(dataBaseDir).resolve(baseKeyspaceName).resolve(baseKeyspaceName + "_4").resolve("byTime");
        Files.createDirectories(path.resolve("chn00"));
        Files.createDirectories(path.resolve("chn01"));
        createChannel(path, "chn02");
        createChannel(path, "chn03");
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
        Set<String> expect = Set.of("chn00", "chn01");
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

}
