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

    void setupData() throws IOException {
        Path path = Path.of(dataBaseDir).resolve(baseKeyspaceName).resolve(baseKeyspaceName + "_4").resolve("byTime");
        Files.createDirectories(path.resolve("chn00"));
        Files.createDirectories(path.resolve("chn01"));
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
