package ch.psi.daq.imageapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;

class ChannelWithFiles {
    String name;
    Utils.MatchingDataFilesResult files;
    public static ChannelWithFiles create(String name, Utils.MatchingDataFilesResult files) {
        ChannelWithFiles ret = new ChannelWithFiles();
        ret.name = name;
        ret.files = files;
        return ret;
    }
    public static ChannelWithFiles fromTuple(Tuple2<String, Utils.MatchingDataFilesResult> tup) {
        return create(tup.getT1(), tup.getT2());
    }
}

@RestController
@RequestMapping("api/v1")
public class Controller {
    static Logger LOGGER = LoggerFactory.getLogger(Controller.class);
    private String rootDir = "/gpfs/sf-data/sf-imagebuffer";
    private String baseKeyspaceName = "daq_swissfel";
    private int binSize = 3600000;
    private FileManager fileManager;

    @PostMapping(path = "query", consumes=MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> query(ServerWebExchange xc, ServerHttpResponse res, @RequestHeader HttpHeaders headers, @RequestBody Query query) {
        RequestStats requestStats = RequestStats.empty(xc);
        Range range = query.getRange();
        Instant begin;
        Instant end;
        if (range instanceof DateRange) {
            begin = ((DateRange)range).getStartDate();
            end = ((DateRange)range).getEndDate();
            if (begin.isAfter(end)) {
                throw new IllegalArgumentException(String.format("Start date %s is before end %s date", begin, end));
            }
            requestStats.rangeBegin = begin;
            requestStats.rangeEnd = end;
        }
        else {
            throw new IllegalArgumentException(String.format("Can not parse request"));
        }

        IFileManager fileManager;
        if (query.getChannels().contains("test01") || headers.containsKey("x-use-test-filemanager")) {
            fileManager = new FileManagerTest();
        }
        else {
            fileManager = filemanagerProduction();
        }

        final ChannelEventStream eventStreamMethod = new ChannelEventStream(res.bufferFactory());

        long t1;
        long t2;
        t2 = System.nanoTime();

        Collection<String> channelNames = fileManager.getChannelNames();
        t1 = t2;
        t2 = System.nanoTime();
        requestStats.getChannelNamesDuration = Microseconds.fromNanos(t2 - t1);

        if (!channelNames.containsAll(query.getChannels())) {
            LOGGER.error("Some channels not found "+ query.getChannels());
            return Mono.just(ResponseEntity.notFound().build());
        }
        t1 = t2;
        t2 = System.nanoTime();
        requestStats.checkAllChannelsContained = Microseconds.fromNanos(t2 - t1);

        long beginNano = Utils.instantToNanoLong(begin);
        long endNano = Utils.instantToNanoLong(end);

        Flux<DataBuffer> responseBody = Flux.fromStream(query.getChannels().stream())
        .flatMapSequential(channelName -> {
            // TODO limit the number of files that can be found to prevent excessive requests
            return Mono.just(channelName).zipWith(Utils.matchingDataFiles(fileManager, channelName, begin, end));
        }, 1)
        .map(ChannelWithFiles::fromTuple)
        .flatMapSequential(channelWithFiles -> eventStreamMethod.bufferFluxFromFiles(channelWithFiles, requestStats, beginNano, endNano), 1)
        .doOnComplete(() -> {
            requestStats.endNow();
            LOGGER.info(toJsonString(requestStats));
        });
        ResponseEntity<Flux<DataBuffer>> responseEntity = ResponseEntity.ok()
        .contentType(MediaType.APPLICATION_OCTET_STREAM)
        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"data.blob\"")
        .body(responseBody);
        return Mono.just(responseEntity);
    }

    @PostMapping(path = "listpulses", consumes=MediaType.APPLICATION_JSON_VALUE, produces = MediaType.TEXT_PLAIN_VALUE)
    public Mono<ResponseEntity<Flux<String>>> listpulses(ServerWebExchange xc, ServerHttpResponse res, @RequestHeader HttpHeaders headers, @RequestBody Query query) {
        RequestStats requestStats = RequestStats.empty(xc);
        Range range = query.getRange();
        Instant begin;
        Instant end;
        if (range instanceof DateRange) {
            begin = ((DateRange)range).getStartDate();
            end = ((DateRange)range).getEndDate();
            if (begin.isAfter(end)) {
                throw new IllegalArgumentException(String.format("Start date %s is before end %s date", begin, end));
            }
            requestStats.rangeBegin = begin;
            requestStats.rangeEnd = end;
        }
        else {
            throw new IllegalArgumentException("Can not parse request");
        }

        IFileManager fileManager;
        if (query.getChannels().contains("test01") || headers.containsKey("x-use-test-filemanager")) {
            fileManager = new FileManagerTest();
        }
        else {
            fileManager = filemanagerProduction();
        }

        final ChannelEventStream eventStreamMethod = new ChannelEventStream(res.bufferFactory());

        long t1;
        long t2;
        t2 = System.nanoTime();

        Collection<String> channelNames = fileManager.getChannelNames();
        t1 = t2;
        t2 = System.nanoTime();
        requestStats.getChannelNamesDuration = Microseconds.fromNanos(t2 - t1);

        if (!channelNames.containsAll(query.getChannels())) {
            LOGGER.error("Some channels not found "+ query.getChannels());
            return Mono.just(ResponseEntity.notFound().build());
        }
        t1 = t2;
        t2 = System.nanoTime();
        requestStats.checkAllChannelsContained = Microseconds.fromNanos(t2 - t1);

        long beginNano = Utils.instantToNanoLong(begin);
        long endNano = Utils.instantToNanoLong(end);

        Flux<String> responseBody = Flux.fromStream(query.getChannels().stream())
        .flatMapSequential(channelName -> {
            // TODO limit the number of files that can be found to prevent excessive requests
            return Mono.just(channelName).zipWith(Utils.matchingDataFiles(fileManager, channelName, begin, end));
        }, 1)
        .map(ChannelWithFiles::fromTuple)
        .flatMapSequential(channelWithFiles -> eventStreamMethod.listPulses(channelWithFiles, requestStats, beginNano, endNano), 1)
        .map(x -> Controller.toJsonString(x) + "\n")
        .doOnComplete(() -> {
            requestStats.endNow();
            LOGGER.info(toJsonString(requestStats));
        });
        ResponseEntity<Flux<String>> responseEntity = ResponseEntity.ok()
        .contentType(MediaType.TEXT_PLAIN)
        .body(responseBody);
        return Mono.just(responseEntity);
    }

    static <T> String toJsonString(T x) {
        ObjectWriter ow = new ObjectMapper().writer();
        try {
            return ow.writeValueAsString(x);
        }
        catch (JsonProcessingException e) {
            return "";
        }
    }

    @GetMapping(path = "q1")
    public Mono<ResponseEntity<Flux<DataBuffer>>> q1(@RequestHeader HttpHeaders headers) {
        return Mono.just(
        ResponseEntity.ok()
        .contentType(MediaType.APPLICATION_OCTET_STREAM)
        .header("h1", "v1")
        .body(Flux.empty())
        );
    }

    @GetMapping(path = "q2", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<String>> q2_json(@RequestHeader HttpHeaders headers) {
        return Mono.just(
        ResponseEntity.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body("{\"answer\":\"ok, json produced\"}")
        );
    }

    @GetMapping(path = "q2", produces = MediaType.TEXT_MARKDOWN_VALUE)
    public Mono<ResponseEntity<String>> q2_md(@RequestHeader HttpHeaders headers, ServerHttpRequest req) {
        return Mono.just(
        ResponseEntity.ok()
        .contentType(MediaType.TEXT_MARKDOWN)
        .body("# Title\n\nMarkdown content.\n")
        );
    }

    @GetMapping(path = "q2")
    public Mono<ResponseEntity<String>> q2_fallback() {
        return Mono.just(
        ResponseEntity.status(HttpStatus.NOT_ACCEPTABLE).build()
        );
    }

    @GetMapping(path = "q3", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> q3(ServerHttpResponse res, @RequestHeader HttpHeaders headers) {
        DataBufferFactory bufFac = res.bufferFactory();
        ByteBuffer buf2 = Utils.allocateByteBuffer(1024);
        buf2.put(new byte[] {0x61, 0x62, 0x63, 0x0a});
        buf2.flip();
        return Mono.just(ResponseEntity.ok().body(Flux.just(bufFac.wrap(buf2))));
    }

    @ExceptionHandler
    public ResponseEntity<String> handle(IOException e, ServerWebExchange xc) {
        return handle((Exception)e, xc);
    }

    @ExceptionHandler
    public ResponseEntity<String> handle(RuntimeException e, ServerWebExchange xc) {
        return handle((Exception)e, xc);
    }

    @ExceptionHandler
    public ResponseEntity<String> handle(Throwable e, ServerWebExchange xc) {
        String requestId = xc.getRequest().getId();
        String method = xc.getRequest().getMethodValue();
        String path = xc.getRequest().getPath().toString();
        LOGGER.error(String.format("[%s] %s  %s  %s", requestId, e.getClass().getName(), method, path), e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(String.format("Internal Server Error for requestId: %s", requestId));
    }

    Mono<ResponseEntity<Flux<ByteBuffer>>> serverErrorResponseAsFlux(ServerWebExchange xc, RequestStats reqst) {
        ResponseEntity<Flux<ByteBuffer>> responseEntity = ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
        .contentType(MediaType.APPLICATION_OCTET_STREAM)
        .build();
        StandardCharsets.UTF_8.encode(String.format("Server Error RequestId: %s", xc.getRequest().getId()));
        return Mono.just(responseEntity);
    }

    void reportException(Throwable e, ServerWebExchange xc) {
        String requestId = xc.getRequest().getId();
        String method = xc.getRequest().getMethodValue();
        String path = xc.getRequest().getPath().toString();
        LOGGER.error(String.format("[%s] %s  %s  %s", requestId, e.getClass().getName(), method, path), e);
    }

    synchronized IFileManager filemanagerProduction() {
        // TODO our FileManager throws if production files are not present.
        // Therefore, need a separate test/prod controller.
        // but for the time being, do it lazy and sync here..
        if (this.fileManager == null) {
            this.fileManager = new FileManager(rootDir, baseKeyspaceName, binSize);
        }
        return FileManagerProd.fromFileManager(this.fileManager);
    }

    @GetMapping(path = "channels", produces = MediaType.APPLICATION_JSON_VALUE)
    public Collection<String> getChannels(@RequestParam(name="regex", required=false) String regex) {
        /*
        TODO do we really want to allow regex?
        Keep it for compatibility here.
        */
        try {
            Predicate<String> regexFilter = x -> true;
            if (regex != null) {
                regexFilter = x -> x.matches(regex);
            }
            return filemanagerProduction().getChannelNames().stream()
            .filter(regexFilter)
            .collect(Collectors.toList());
        }
        catch (java.util.regex.PatternSyntaxException e) {
            LOGGER.error("Regex error on {}", regex);
            throw e;
        }
    }

    @GetMapping(path = "channel/{channelName}", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<ChannelConfig> getChannelConfig(@PathVariable String channelName) {
        Path path = filemanagerProduction().locateConfigFile(channelName);
        if (path == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Config for " + channelName + " not found");
        }
        try {
            return ChannelConfigFileReader.read(path, null, null);
        }
        catch (IOException e) {
            // TODO
            LOGGER.error("getChannelConfig  e: {}", e.toString());
            throw new RuntimeException("getChannelConfig");
        }
    }

}
