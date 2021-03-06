package ch.psi.daq.imageapi.controller;

import ch.psi.daq.imageapi.*;
import ch.psi.daq.imageapi.finder.BaseDirFinderFormatV0;
import ch.psi.daq.imageapi.pod.api1.ChannelConfigSearchQuery;
import ch.psi.daq.imageapi.pod.api1.ChannelSearchQuery;
import ch.psi.daq.imageapi.pod.api1.Order;
import ch.psi.daq.imageapi.pod.api1.Query;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.io.buffer.*;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.Function;

@RestController
@RequestMapping("api/1.0.1")
public class API_1_0_1 implements ApplicationListener<WebServerInitializedEvent> {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger("API_1_0_1");
    @Value("${imageapi.dataBaseDir:UNDEFINED}") private String dataBaseDir;
    @Value("${imageapi.baseKeyspaceName:UNDEFINED}") private String baseKeyspaceName;
    @Value("${imageapi.configFile:UNDEFINED}") String configFile;
    public QueryData queryData;
    public int localPort;
    ConfigurationRetrieval conf;
    InetAddress localAddress = null;
    String localAddressString;
    String localHostname;
    String canonicalHostname;
    static Scheduler dbsched = Schedulers.newParallel("db", 32);
    {
        try {
            localAddress = InetAddress.getLocalHost();
            localHostname = localAddress.getHostName();
            canonicalHostname = localAddress.getCanonicalHostName();
        }
        catch (UnknownHostException e) {
            localHostname = "UNKNOWNHOSTNAME";
        }
        localAddressString = String.format("%s", localAddress);
    }
    DataBufferFactory defaultDataBufferFactory = new DefaultDataBufferFactory();

    @PostMapping(path = "query", consumes = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> query(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        // The default is octets, to stay compatible with older clients
        LOGGER.info("/query via default endpoint");
        if (exchange.getRequest().getHeaders().getAccept().contains(MediaType.APPLICATION_OCTET_STREAM)) {
            LOGGER.info("Started in default endpoint despite having octet-stream set");
        }
        return queryProducesOctets(exchange, queryMono);
    }

    @PostMapping(path = "query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryProducesOctets(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        if (conf.mergeLocal) {
            return queryData.queryMergedLocal(exchange, queryMono);
        }
        else {
            return queryData.queryMergedOctets(exchange, queryMono);
        }
    }

    @PostMapping(path = "query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryProducesJson(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        if (!exchange.getRequest().getHeaders().getAccept().contains(MediaType.APPLICATION_JSON)) {
            LOGGER.warn("/query for json without Accept header");
        }
        return queryData.queryMergedJson(exchange, queryMono);
    }

    @PostMapping(path = "queryMerged", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedOctets(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        return queryData.queryMergedOctets(exchange, queryMono);
    }

    @PostMapping(path = "queryLocal", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryLocal(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        return queryData.queryLocal(exchange, queryMono);
    }

    @PostMapping(path = "rng", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> rng(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        final int N = 64 * 1024;
        byte[] load = new byte[N];
        DataBufferFactory bufFac = defaultDataBufferFactory;
        Flux<DataBuffer> mret = queryMono
        .doOnError(x -> LOGGER.info("can not parse request"))
        .flatMapMany(query -> Flux.generate(() -> 0L, (st, si) -> {
            byte v = (byte) (0xff & st);
            Arrays.fill(load, v);
            DataBuffer buf = bufFac.allocateBuffer(N);
            buf.write(load);
            si.next(buf);
            return 1 + st;
        }));
        return Mono.just(ResponseEntity.ok().contentType(MediaType.APPLICATION_OCTET_STREAM).body(mret));
    }

    @PostMapping(path = "rawLocal", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> rawLocal(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        return queryData.rawLocal(exchange, queryMono);
    }

    @PostMapping(path = "queryMergedLocal", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedLocalOctets(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        return queryData.queryMergedLocal(exchange, queryMono);
    }

    @PostMapping(path = "queryJson", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryJson(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        return queryMergedJson(exchange, queryMono);
    }

    @PostMapping(path = "queryMergedJson", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedJson(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        return queryData.queryMergedJson(exchange, queryMono);
    }

    Flux<DataBuffer> channelsJson(DataBufferFactory bufFac, ChannelConfigSearchQuery q, boolean configOut) {
        return Flux.generate(() -> ChannelLister.create(conf, bufFac, q.order(), q.regex, q.sourceRegex, q.descriptionRegex, configOut), ChannelLister::generate, ChannelLister::release)
        .subscribeOn(dbsched)
        .flatMapIterable(Function.identity());
    }

    @GetMapping(path = "channels", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> channelsGet(ServerWebExchange exchange) {
        LOGGER.info("Request for channelsGet");
        ChannelConfigSearchQuery q = new ChannelConfigSearchQuery();
        q.ordering = "asc";
        return Mono.just(channelsJson(exchange.getResponse().bufferFactory(), q, false))
        .map(fl -> {
            LOGGER.info("Building response entity");
            return ResponseEntity.ok()
            .header("X-CanonicalHostname", canonicalHostname)
            .contentType(MediaType.APPLICATION_JSON)
            .body(fl);
        });
    }

    @GetMapping(path = "channels/search/regexp/{regexp}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> channelsGet(ServerWebExchange exchange, @PathVariable String regexp) {
        LOGGER.info("Request for channelsRegexp  [{}]", regexp);
        ChannelConfigSearchQuery q = new ChannelConfigSearchQuery();
        q.ordering = "asc";
        return Mono.just(channelsJson(exchange.getResponse().bufferFactory(), q, false))
        .map(fl -> {
            return ResponseEntity.ok()
            .header("X-CanonicalHostname", canonicalHostname)
            .contentType(MediaType.APPLICATION_JSON)
            .body(fl);
        });
    }

    @PostMapping(path = "channels", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> channelsPost(ServerWebExchange exchange, @RequestBody Mono<ChannelConfigSearchQuery> queryMono) {
        LOGGER.info("Request for channelsPost");
        return queryMono.map(query -> {
            if (!query.valid()) {
                throw new RuntimeException("invalid query");
            }
            LOGGER.info("regex: {}", query.regex);
            return channelsJson(exchange.getResponse().bufferFactory(), query, false);
        })
        .map(fl -> {
            return ResponseEntity.ok()
            .header("X-CanonicalHostname", canonicalHostname)
            .contentType(MediaType.APPLICATION_JSON)
            .body(fl);
        });
    }

    @PostMapping(path = "channels/config", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> channelsConfigPost(ServerWebExchange exchange, @RequestBody Mono<ChannelConfigSearchQuery> queryMono) {
        LOGGER.info("Request for channelsPost");
        return queryMono.map(query -> {
            if (!query.valid()) {
                throw new RuntimeException("invalid query");
            }
            LOGGER.info("regex: {}", query.regex);
            return channelsJson(exchange.getResponse().bufferFactory(), query, true);
        })
        .map(fl -> {
            return ResponseEntity.ok()
            .header("X-CanonicalHostname", canonicalHostname)
            .contentType(MediaType.APPLICATION_JSON)
            .body(fl);
        });
    }

    @GetMapping(path = "paramsList/{params}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String channelsGet(ServerWebExchange paramsList, @PathVariable List<String> params) {
        return String.format("len %d  %s", params.size(), params.toString());
    }

    @GetMapping(path = "paramsMap/{params}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String channelsGet(ServerWebExchange paramsMap, @PathVariable Map<String, String> params) {
        return String.format("len %d  %s", params.size(), params.toString());
    }

    public static void logHeaders(ServerWebExchange ex) {
        for (String n : List.of("User-Agent", "X-PythonDataAPIPackageVersion", "X-PythonDataAPIModule")) {
            LOGGER.info("req {}  {} {}", ex.getRequest().getId(), n, ex.getRequest().getHeaders().get(n));
        }
    }

    @GetMapping(path = "requestStatus/{reqid}", produces = MediaType.APPLICATION_JSON_VALUE)
    public RequestStatus requestStatus(ServerWebExchange exchange, @PathVariable String reqid) {
        LOGGER.info("Request for requestStatus  {}", reqid);
        return queryData.reqStatus(reqid);
    }

    ConfigurationRetrieval loadConfiguration(File f1) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new JsonFactory());
        ConfigurationRetrieval conf = mapper.readValue(f1, ConfigurationRetrieval.class);
        if (conf.splitNodes != null) {
            for (SplitNode sn : conf.splitNodes) {
                if (sn.host == null) {
                    sn.host = "localhost";
                }
                if (sn.port == 0) {
                    sn.port = localPort;
                }
            }
        }
        return conf;
    }

    ConfigurationRetrieval loadConfiguration(WebServerInitializedEvent ev) throws IOException {
        if (configFile != null && !configFile.equals("UNDEFINED")) {
            LOGGER.info("try file: {}", configFile);
            File f1 = ResourceUtils.getFile(configFile);
            LOGGER.info("load from: {}", f1);
            return loadConfiguration(f1);
        }
        else {
            try {
                File f1 = ResourceUtils.getFile("classpath:retrieval.json");
                LOGGER.info("load from: {}", f1);
                return loadConfiguration(f1);
            }
            catch (Exception e) {
                LOGGER.info("no default configFile found.");
            }
        }
        return null;
    }

    @Override
    public void onApplicationEvent(WebServerInitializedEvent ev) {
        List<SplitNode> splitNodes = List.of();
        localPort = ev.getWebServer().getPort();
        try {
            ConfigurationRetrieval conf = loadConfiguration(ev);
            conf.validate();
            LOGGER.info("loaded: {}", conf);
            if (conf != null) {
                this.conf = conf;
                splitNodes = conf.splitNodes;
            }
        }
        catch (ConfigurationRetrieval.InvalidException e) {
            LOGGER.error("Invalid configuration: {}", e.toString());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        Hooks.onNextDropped(obj -> {
            LOGGER.error("Hooks.onNextDropped  {}", obj);
        });
        Hooks.onOperatorError((err, obj) -> {
            LOGGER.error("Hooks.onOperatorError  {}", obj);
            return err;
        });
        LOGGER.info("localPort {}  dataBaseDir {}", localPort, dataBaseDir);
        queryData = new QueryData(new BaseDirFinderFormatV0(dataBaseDir, baseKeyspaceName), splitNodes, canonicalHostname);
    }

    public long getTotalBytesServed() {
        return queryData.totalBytesServed.get();
    }

}
