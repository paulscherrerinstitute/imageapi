package ch.psi.daq.imageapi.controller;

import ch.psi.daq.imageapi.*;
import ch.psi.daq.imageapi.eventmap.ts.EventBlobToV1MapTs;
import ch.psi.daq.imageapi.eventmap.ts.Item;
import ch.psi.daq.imageapi.eventmap.value.EventBlobMapResult;
import ch.psi.daq.imageapi.eventmap.value.EventBlobToV1Map;
import ch.psi.daq.imageapi.finder.BaseDirFinderFormatV0;
import ch.psi.daq.imageapi.merger.Merger;
import ch.psi.daq.imageapi.pod.api1.Query;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;

@RestController
@RequestMapping("api/1.0.1")
public class API_1_0_1 implements ApplicationListener<WebServerInitializedEvent> {
    static Logger LOGGER = LoggerFactory.getLogger("API_1_0_1");
    @Value("${imageapi.dataBaseDir}") private String dataBaseDir;
    @Value("${imageapi.baseKeyspaceName}") private String baseKeyspaceName;
    @Value("${imageapi.configFile:UNDEFINED}") String configFile;
    @Value("${imageapi.nodeId:-1}") int nodeId;
    int bufferSize = 16 * 1024;
    public BaseDirFinderFormatV0 baseDirFinder;
    public int localPort;
    List<SplitNode> splitNodes;
    RetrievalConfiguration conf;
    InetAddress localAddress = null;
    String localAddressString;
    String localHostname;
    String canonicalHostname;
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

    @PostMapping(path = "query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> query(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        // So far just an alias
        return queryLocal(exchange, queryMono);
    }

    @PostMapping(path = "queryLocal", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryLocal(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        DataBufferFactory bufFac = exchange.getResponse().bufferFactory();
        ServerHttpRequest req = exchange.getRequest();
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.info("can not parse request"))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(query);
            LOGGER.info(String.format("queryLocal  %s  %s  %s  %s", req.getId(), qp.begin, qp.end, qp.channels));
            long endNanos = 1000000L * qp.end.toEpochMilli();
            class MakeTrans implements MapFunctionFactory<EventBlobMapResult> {
                QueryParams qp;
                long endNanos;
                public MakeTrans(QueryParams qp, long endNanos) {
                    this.qp = qp;
                    this.endNanos = endNanos;
                }
                @Override
                public Function<Flux<DataBuffer>, Publisher<EventBlobMapResult>> makeTrans(KeyspaceToDataParams kspp, int fileno) {
                    return EventBlobToV1Map.trans(kspp.ksp.channel.name, endNanos, kspp.bufFac, kspp.bufferSize, qp.decompressOnServer, qp.limitBytes);
                }
            }
            Function<KeyspaceToDataParams, Mono<List<Flux<EventBlobMapResult>>>> keyspaceToData = p -> {
                return ChannelEventStream.dataFluxFromFiles(p, new MakeTrans(qp, endNanos));
            };
            return channelsToData(baseDirFinder, exchange.getRequest(), qp.channels, qp.begin, qp.end, qp.splits, bufFac, bufferSize, keyspaceToData)
            .map(x -> x.buf);
        });
        return logResponse("queryLocal", mret, req);
    }

    @PostMapping(path = "rawLocal", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> rawLocal(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        DataBufferFactory bufFac = exchange.getResponse().bufferFactory();
        ServerHttpRequest req = exchange.getRequest();
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.info("can not parse request"))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(query);
            LOGGER.info(String.format("%s  rawLocal  %s  %s  %s", req.getId(), qp.begin, qp.end, qp.channels));
            long endNanos = 1000000L * qp.end.toEpochMilli();
            class MakeTrans implements MapFunctionFactory<Item> {
                QueryParams qp;
                long endNanos;
                public MakeTrans(QueryParams qp, long endNanos) {
                    this.qp = qp;
                    this.endNanos = endNanos;
                }
                @Override
                public Function<Flux<DataBuffer>, Publisher<Item>> makeTrans(KeyspaceToDataParams kspp, int fileno) {
                    return EventBlobToV1MapTs.trans(String.format("rawLocal_sp%02d/%d_f%02d", qp.splits.get(0), qp.splits.size(), fileno), endNanos, kspp.bufFac, qp.bufferSize, 1);
                }
            }
            Function<KeyspaceToDataParams, Mono<List<Flux<Item>>>> keyspaceToData = p -> {
                return ChannelEventStream.dataFluxFromFiles(p, new MakeTrans(qp, endNanos));
            };
            return channelsToData(baseDirFinder, exchange.getRequest(), qp.channels, qp.begin, qp.end, qp.splits, bufFac, qp.bufferSize, keyspaceToData)
            .map(item -> {
                if (item.isTerm()) {
                    if (item.item1 != null) {
                        if (item.item1.buf != null) {
                            DataBufferUtils.release(item.item1.buf);
                            item.item1.buf = null;
                        }
                    }
                    if (item.item2 != null) {
                        if (item.item2.buf != null) {
                            DataBufferUtils.release(item.item2.buf);
                            item.item2.buf = null;
                        }
                    }
                }
                return item;
            })
            .doOnDiscard(Item.class, item -> {
                LOGGER.warn("DISCARD ITEM in rawLocal");
                item.release();
            })
            .takeWhile(item -> !item.isTerm())
            .flatMapIterable(item -> {
                if (item.item2 != null) {
                    return List.of(item.item1.buf, item.item2.buf);
                }
                else if (item.item1 != null) {
                    return List.of(item.item1.buf);
                }
                else {
                    LOGGER.warn("{}  rawLocal empty item", req.getId());
                    return List.of();
                }
            }, 1)
            .doOnError(e -> LOGGER.error("{}  rawLocal error2: {}", req.getId(), e.toString()));
        });
        return logResponse("rawLocal", mret, req);
    }

    @PostMapping(path = "queryMerged", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMerged(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        DataBufferFactory bufFac = exchange.getResponse().bufferFactory();
        ServerHttpRequest req = exchange.getRequest();
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.info("can not parse request"))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(query);
            LOGGER.info(String.format("%s  queryMerged  %s  %s  %s", req.getId(), qp.begin, qp.end, qp.channels));
            Instant begin = Instant.parse(query.range.startDate);
            Instant end = Instant.parse(query.range.endDate);
            long endNanos = 1000000L * end.toEpochMilli();
            Flux<Mono<Flux<EventBlobMapResult>>> fcmf = Flux.fromIterable(query.channels)
            .map(channelName -> {
                Flux<Mono<Flux<Item>>> fmf = Flux.fromIterable(splitNodes)
                .filter(sn -> qp.splits == null || qp.splits.isEmpty() || qp.splits.contains(sn.split))
                .map(sn -> {
                    LOGGER.info("sn {}", sn.split);
                    Query subq = new Query();
                    subq.decompressOnServer = 0;
                    subq.bufferSize = query.bufferSize;
                    subq.channels = List.of(channelName);
                    subq.range = query.range;
                    subq.splits = List.of(sn.split);
                    String js;
                    try {
                        ObjectMapper mapper = new ObjectMapper(new JsonFactory());
                        js = mapper.writeValueAsString(subq);
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    String localURL = String.format("http://%s:%d/api/1.0.1/rawLocal", sn.host, sn.port);
                    LOGGER.info("{}  localURL: {}", req.getId(), localURL);
                    LOGGER.info("{}  request data: {}", req.getId(), js);
                    Mono<Flux<Item>> m3 = WebClient.builder()
                    .baseUrl(localURL)
                    .build()
                    .post()
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_OCTET_STREAM)
                    .body(BodyInserters.fromValue(js))
                    .exchange()
                    .doOnNext(x -> {
                        if (x.statusCode() != HttpStatus.OK) {
                            throw new RuntimeException("sub request not OK");
                        }
                    })
                    .map(x -> {
                        Flux<Item> fitem = x.bodyToFlux(DataBuffer.class)
                        .doOnDiscard(DataBuffer.class, DataBufferUtils::release)
                        .transform(EventBlobToV1MapTs.trans(String.format("__sn%02d__queryMerged", sn.split), endNanos, bufFac, bufferSize, 2));
                        return logFlux(String.format("merged_sn%02d_ts_flux_ts", sn.split), req, fitem);
                    });
                    return logMono(String.format("merged_sn%02d_ts_mono_sub", sn.split), req, m3);
                });
                Flux<Mono<Flux<Item>>> fmf2 = logFlux("merged_ts_flux_subs", req, fmf);
                Mono<Flux<EventBlobMapResult>> fl4 = fmf2.flatMapSequential(Function.identity(), 1, 1)
                .collectList()
                .map(lfl -> Flux.from(new Merger(lfl, bufFac, bufferSize)))
                .map(x -> {
                    return x
                    .transform(EventBlobToV1Map.trans(channelName, endNanos, bufFac, bufferSize, query.decompressOnServer == 1, qp.limitBytes))
                    .doOnNext(x2 -> {
                        if (x2.term) {
                            LOGGER.warn("EventBlobToV1Map reached TERM");
                        }
                    })
                    .takeWhile(x2 -> !x2.term);
                });
                return fl4;
            });
            Flux<Flux<EventBlobMapResult>> fl5 = fcmf.flatMapSequential(x -> x, 1, 1);
            Flux<EventBlobMapResult> fl6 = fl5.flatMapSequential(x -> x, 1, 1);
            Flux<DataBuffer> fl7 = fl6.map(x -> x.buf);
            return fl7;
        });
        return logResponse("queryMerged", mret, req);
    }

    static <T> Mono<T> logMono(String name, ServerHttpRequest req, Mono<T> m) {
        return m
        .doOnSuccess(x -> {
            LOGGER.info(String.format("%s  %s  success", req.getId(), name));
        })
        .doOnCancel(() -> {
            LOGGER.info(String.format("%s  %s  cancel", req.getId(), name));
        })
        .doOnError(e -> {
            LOGGER.info(String.format("%s  %s  error %s", req.getId(), name, e));
        })
        .doOnTerminate(() -> {
            LOGGER.info(String.format("%s  %s  terminate", req.getId(), name));
        });
    }

    static <T> Flux<T> logFlux(String name, ServerHttpRequest req, Flux<T> m) {
        return m
        .doOnComplete(() -> {
            LOGGER.info(String.format("%s  %s  complete", req.getId(), name));
        })
        .doOnCancel(() -> {
            LOGGER.info(String.format("%s  %s  cancel", req.getId(), name));
        })
        .doOnError(e -> {
            LOGGER.info(String.format("%s  %s  error %s", req.getId(), name, e));
        })
        .doOnTerminate(() -> {
            LOGGER.info(String.format("%s  %s  terminate", req.getId(), name));
        });
    }

    <T> Mono<ResponseEntity<Flux<T>>> logResponse(String name, Mono<Flux<T>> m, ServerHttpRequest req) {
        return logMono(name, req, m.map(x -> {
            return ResponseEntity.ok()
            .header("X-NodeId", String.format("%d", nodeId))
            .header("X-CanonicalHostname", canonicalHostname)
            .contentType(MediaType.APPLICATION_OCTET_STREAM)
            .body(x);
        }));
    }

    static <T> Flux<T> channelsToData(BaseDirFinderFormatV0 baseDirFinder, ServerHttpRequest req, List<String> channels, Instant begin, Instant end, List<Integer> splits, DataBufferFactory bufFac, int bufferSize, Function<KeyspaceToDataParams, Mono<List<Flux<T>>>> keyspaceToData) {
        Flux<T> ret = Flux.fromIterable(channels)
        .flatMapSequential(channelName -> {
            Flux<T> bulk = baseDirFinder.findMatchingDataFiles(channelName, begin, end, splits, bufFac)
            .doOnNext(x -> {
                if (x.keyspaces.size() < 1) {
                    LOGGER.warn(String.format("no keyspace found for channel %s", channelName));
                }
                else if (x.keyspaces.size() > 1) {
                    LOGGER.warn(String.format("more than one keyspace for %s", channelName));
                }
                else {
                    if (false) {
                        LOGGER.info("Channel {} using files: {}", channelName, x.keyspaces.get(0).splits.stream().map(sp -> sp.timeBins.size()).reduce(0, (a2, x2) -> a2 + x2));
                    }
                }
            })
            .flatMapIterable(x2 -> x2.keyspaces)
            .flatMapSequential(ks -> keyspaceToData.apply(new KeyspaceToDataParams(ks, begin, end, bufFac, bufferSize, splits, req)), 1, 1)
            .flatMapSequential(x -> {
                if (x.size() != 1) {
                    throw new RuntimeException("not yet supported in local query");
                }
                return x.get(0);
            }, 1, 1);
            return bulk;
        }, 1, 1)
        .doOnDiscard(Object.class, x -> LOGGER.info("doOnDiscard channelsToData"));
        Flux<T> ret2 = logFlux("channelsToData", req, ret);
        return ret2;
    }

    RetrievalConfiguration loadConfiguration(File f1) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new JsonFactory());
        RetrievalConfiguration conf = mapper.readValue(f1, RetrievalConfiguration.class);
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

    RetrievalConfiguration loadConfiguration(WebServerInitializedEvent ev) throws IOException {
        //LOGGER.info("Path: {}", Path.of().toAbsolutePath());
        //URL uri = ClassLoader.getSystemResource("retrieval.json");
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
        localPort = ev.getWebServer().getPort();
        try {
            RetrievalConfiguration conf = loadConfiguration(ev);
            LOGGER.info("loaded: {}", conf);
            if (conf != null) {
                this.conf = conf;
                splitNodes = conf.splitNodes;
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        baseDirFinder = new BaseDirFinderFormatV0(dataBaseDir, baseKeyspaceName);
    }

}
