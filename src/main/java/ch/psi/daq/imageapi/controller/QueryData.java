package ch.psi.daq.imageapi.controller;

import ch.psi.daq.imageapi.*;
import ch.psi.daq.imageapi.eventmap.ts.EventBlobToV1MapTs;
import ch.psi.daq.imageapi.eventmap.ts.Item;
import ch.psi.daq.imageapi.eventmap.value.*;
import ch.psi.daq.imageapi.finder.BaseDirFinderFormatV0;
import ch.psi.daq.imageapi.merger.Merger;
import ch.psi.daq.imageapi.pod.api1.Query;
import ch.psi.daq.imageapi.pod.api1.Range;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class QueryData {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger(QueryData.class);
    DataBufferFactory defaultDataBufferFactory = new DefaultDataBufferFactory();
    int bufferSize = 64 * 1024;
    public BaseDirFinderFormatV0 baseDirFinder;
    AtomicLong nFilteredEmptyRawOut = new AtomicLong();
    List<SplitNode> splitNodes;
    AtomicLong totalBytesServed = new AtomicLong();
    String canonicalHostname;
    final Map<String, RequestStatus> requestStatuses = new HashMap<>();

    static Marker logMarkerWebClientResponse = MarkerFactory.getMarker("WebClientResponse");
    static Marker logMarkerWebClientResponseItem = MarkerFactory.getMarker("WebClientResponseItem");
    static Marker logMarkerQueryMergedItems = MarkerFactory.getMarker("QueryMergedItems");
    static Marker logMarkerRawLocalItem = MarkerFactory.getMarker("RawLocalItem");
    static Marker logMarkerAccept = MarkerFactory.getMarker("Accept");

    static class ItemFilter extends TurboFilter {
        @Override
        public FilterReply decide(Marker marker, Logger logger, Level level, String format, Object[] objs, Throwable err) {
            if (marker != null) {
                if (marker.equals(logMarkerRawLocalItem)) {
                    return FilterReply.DENY;
                }
                if (marker.equals(logMarkerWebClientResponseItem)) {
                    return FilterReply.DENY;
                }
                if (marker.equals(logMarkerAccept)) {
                    return FilterReply.ACCEPT;
                }
            }
            return FilterReply.NEUTRAL;
        }
    }

    static {
        LOGGER.getLoggerContext().addTurboFilter(new ItemFilter());
    }

    static class Timeout1 extends RuntimeException {}
    static class Timeout2 extends RuntimeException {}
    static class Timeout3 extends RuntimeException {}

    static Scheduler clientsplitnodeiter = Schedulers.newParallel("csn", 16);

    public QueryData(BaseDirFinderFormatV0 finder, List<SplitNode> splitNodes, String canonicalHostname) {
        this.baseDirFinder = finder;
        this.splitNodes = splitNodes;
        this.canonicalHostname = canonicalHostname;
    }

    void cleanStatusLog() {
        synchronized (requestStatuses) {
            cleanGivenStatusLog(requestStatuses, ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(120), 10000);
        }
    }

    public static void cleanGivenStatusLog(Map<String, RequestStatus> map, ZonedDateTime keepTo, int keepMax) {
        List<ZonedDateTime> tss = new ArrayList<>();
        for (String k : map.keySet()) {
            RequestStatus s = map.get(k);
            tss.add(s.timestamp);
        }
        tss.sort((a, b) -> {
            if (a.equals(b)) {
                return 0;
            }
            if (a.isAfter(b)) {
                return -1;
            }
            return +1;
        });
        int i1 = 0;
        ZonedDateTime thresh = null;
        for (ZonedDateTime t1 : tss) {
            if (i1 >= keepMax || t1.isBefore(keepTo)) {
                thresh = t1;
                break;
            }
            i1 += 1;
        }
        if (thresh != null) {
            List<String> l1 = new ArrayList<>();
            for (String k : map.keySet()) {
                RequestStatus s = map.get(k);
                if (s.timestamp.isBefore(thresh) || s.timestamp.isEqual(thresh)) {
                    l1.add(k);
                }
            }
            for (String k : l1) {
                map.remove(k);
            }
        }
    }

    int cleanCount = 0;

    @Scheduled(fixedRate = 4000)
    void scheduledStatusClean() {
        cleanCount += 1;
        int n1;
        synchronized (requestStatuses) {
            n1 = requestStatuses.size();
        }
        if (n1 > 0) {
            cleanCount = 0;
            LOGGER.info("call cleanStatusLog");
            cleanStatusLog();
        }
        else if (cleanCount > 0) {
            cleanCount = 0;
            LOGGER.info("scheduledStatusClean nothing to do");
        }
    }

    public RequestStatus reqStatus(String reqId) {
        synchronized (requestStatuses) {
            return requestStatuses.get(reqId);
        }
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryLocal(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        ServerHttpRequest req = exchange.getRequest();
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.info("can not parse request"))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(query, defaultDataBufferFactory, bufferSize);
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
                public Flux<EventBlobMapResult> makeTrans(Flux<DataBuffer> fl, KeyspaceToDataParams kspp, int fileno) {
                    return EventBlobToV1Map.trans2(fl, kspp.ksp.channel.name, endNanos, kspp.bufFac, kspp.bufferSize, qp.decompressOnServer, qp.limitBytes);
                }
            }
            Function<KeyspaceToDataParams, Mono<List<Flux<EventBlobMapResult>>>> keyspaceToData = p -> {
                return ChannelEventStream.dataFluxFromFiles(p, new MakeTrans(qp, endNanos));
            };
            return channelsToData(baseDirFinder, exchange.getRequest(), qp.channels, qp.begin, qp.end, qp.splits, qp.bufFac, qp.bufferSize, keyspaceToData)
            .doOnDiscard(EventBlobMapResult.class, obj -> obj.release())
            .map(x -> x.buf);
        });
        return logResponse("queryLocal", mret, req);
    }

    static class TransMapTsForRaw implements MapFunctionFactory<Item> {
        QueryParams qp;
        long endNanos;
        String channelName;
        public TransMapTsForRaw(QueryParams qp, long endNanos, String channelName) {
            this.qp = qp;
            this.endNanos = endNanos;
            this.channelName = channelName;
        }
        @Override
        public Flux<Item> makeTrans(Flux<DataBuffer> fl, KeyspaceToDataParams kspp, int fileno) {
            return EventBlobToV1MapTs.trans2(EventBlobToV1MapTs.Mock.NONE, fl, String.format("rawLocal_sp%02d/%d_f%02d", qp.splits.get(0), qp.splits.size(), fileno), channelName, endNanos, kspp.bufFac, qp.bufferSize);
        }
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> rawLocal(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        ServerHttpRequest req = exchange.getRequest();
        synchronized (requestStatuses) {
            RequestStatus stat = new RequestStatus();
            requestStatuses.put(req.getId(), stat);
        }
        DataBufferFactory bufFac = exchange.getResponse().bufferFactory();
        AtomicLong totalBytesEmit = new AtomicLong();
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.info("can not parse request"))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(query, defaultDataBufferFactory, bufferSize);
            if (qp.channels.size() != 1) {
                throw new RuntimeException("logic");
            }
            final String channelName = "" + qp.channels.get(0);
            LOGGER.info("{}  rawLocal  {}  {}  {}", req.getId(), qp.begin, qp.end, qp.channels);
            long endNanos = 1000000L * qp.end.toEpochMilli();
            Function<KeyspaceToDataParams, Mono<List<Flux<Item>>>> keyspaceToData = p -> {
                return ChannelEventStream.dataFluxFromFiles(p, new TransMapTsForRaw(qp, endNanos, channelName));
            };
            return channelsToData(baseDirFinder, exchange.getRequest(), qp.channels, qp.begin, qp.end, qp.splits, qp.bufFac, qp.bufferSize, keyspaceToData)
            .doOnNext(item -> {
                if (item.isTerm()) {
                    LOGGER.info("IS TERM  channel {}", channelName);
                    item.release();
                }
            })
            .doOnDiscard(Item.class, item -> {
                LOGGER.warn("DISCARD ITEM in rawLocal");
                item.release();
            })
            .takeWhile(item -> !item.isTerm())
            .flatMapIterable(item -> item.takeBuffers())
            .doOnNext(buf -> totalBytesEmit.getAndAdd(buf.readableByteCount()))
            .doOnSubscribe(s -> {
                LOGGER.info("{}  rawLocal sig  SUBSCRIBE  {} {}", req.getId(), qp.channels.size(), qp.channels.get(0));
            })
            .doOnCancel(() -> {
                LOGGER.info("{}  rawLocal sig  CANCEL     {} {}", req.getId(), qp.channels.size(), qp.channels.get(0));
            })
            .doOnComplete(() -> {
                LOGGER.info("{}  rawLocal sig  COMPLETE   {} {}  bytes {}", req.getId(), qp.channels.size(), qp.channels.get(0), totalBytesEmit.get());
            })
            .doOnTerminate(() -> {
                LOGGER.info("{}  rawLocal sig  TERMINATE  {} {}", req.getId(), qp.channels.size(), qp.channels.get(0));
            })
            .doOnError(e -> {
                LOGGER.error("{}  rawLocal  ERROR  {}", req.getId(), e.toString());
                synchronized (requestStatuses) {
                    RequestStatus st = requestStatuses.get(req.getId());
                    if (st == null) {
                        st = new RequestStatus();
                        requestStatuses.put(req.getId(), st);
                    }
                    if (st.errors == null) {
                        st.errors = new ArrayList<>();
                    }
                    RequestStatus.Error err = new RequestStatus.Error();
                    err.msg = String.format("channel %s   %s", qp.channels.get(0), e.toString());
                    st.errors.add(err);
                }
            })
            .filter(buf -> {
                if (buf.readableByteCount() > 0) {
                    return true;
                }
                else {
                    nFilteredEmptyRawOut.getAndAdd(1);
                    DataBufferUtils.release(buf);
                    return false;
                }
            });
        })
        .map(fl -> {
            return Flux.just(bufFac.wrap(new byte[] { 'P', 'R', 'E', '0' }))
            .concatWith(fl);
        })
        .doOnError(e -> {
            LOGGER.error("{}  rawLocal ERROR  {}", req.getId(), e.toString());
            synchronized (requestStatuses) {
                RequestStatus st = requestStatuses.get(req.getId());
                if (st == null) {
                    st = new RequestStatus();
                    requestStatuses.put(req.getId(), st);
                }
                if (st.errors == null) {
                    st.errors = new ArrayList<>();
                }
                RequestStatus.Error err = new RequestStatus.Error();
                err.msg = String.format("%s", e.toString());
                st.errors.add(err);
            }
        });
        return logResponse("rawLocal", mret, req);
    }


    static class TokenSinker {
        BlockingQueue<Integer> queue;
        AtomicLong n1 = new AtomicLong();
        TokenSinker(int n) {
            queue = new LinkedBlockingQueue<>(n+8);
            for (int i1 = 0; i1 < n; i1 += 1) {
                putBack(i1);
            }
        }
        void putBack(int token) {
            queue.add((((int)n1.getAndAdd(1)) * 10000) + (token % 10000));
        }
    }

    Mono<ClientResponse> springWebClientRequest(String localURL, String channelName, String js, ServerHttpRequest req) {
        return WebClient.builder()
        .baseUrl(localURL)
        .build()
        .post()
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_OCTET_STREAM)
        .header("Connection", "close")
        .body(BodyInserters.fromValue(js))
        .exchange()
        .doOnError(e -> {
            LOGGER.error("WebClient exchange doOnError {}", e.toString());
        })
        .doOnNext(x -> {
            String remote_reqid = x.headers().header("x-daqbuffer-request-id").get(0);
            LOGGER.info("{}  WebClient got status {}  {}  remote x-daqbuffer-request-id {}", req.getId(), x.statusCode(), channelName, remote_reqid);
            if (x.statusCode() != HttpStatus.OK) {
                LOGGER.error("{}  WebClient got status {}  {}", req.getId(), x.statusCode(), channelName);
                throw new RuntimeException("sub request not OK");
            }
            if (!x.headers().header("connection").contains("close")) {
                LOGGER.error("{}  WebClient no conn close header  {}", req.getId(), channelName);
                for (Map.Entry<String, List<String>> e : x.headers().asHttpHeaders().entrySet()) {
                    LOGGER.info("header: {}", e.getKey());
                    for (String v : e.getValue()) {
                        LOGGER.info("  v: {}", v);
                    }
                }
                LOGGER.warn("{}", x.headers().toString());
                throw new RuntimeException("Expect Connection close in answer");
            }
        });
    }

    Flux<DataBuffer> pipeThroughMergerFake(List<Flux<Item>> lfl, String channelName, QueryParams qp) {
        return Flux.concat(lfl)
        .map(item -> {
            if (item.item1 == null) {
                throw new RuntimeException("item1 is null");
            }
            DataBuffer ret = item.item1.buf;
            if (ret == null) {
                LOGGER.error("null item");
                throw new RuntimeException("bad null");
            }
            if (item.item2 != null && item.item2.buf != null) {
                DataBufferUtils.release(item.item2.buf);
            }
            return ret;
        });
    }

    Flux<DataBuffer> pipeThroughMerger(List<Flux<Item>> lfl, String channelName, QueryParams qp) {
        AtomicLong totalSeenItems = new AtomicLong();
        AtomicLong totalRequestedItems = new AtomicLong();
        return Flux.from(new Merger(channelName, lfl, qp.bufFac, qp.bufferSize))
        .doOnRequest(n -> {
            if (n > 50000) {
                LOGGER.warn("large item request {}", n);
            }
            long tot = totalRequestedItems.addAndGet(n);
            long rec = totalSeenItems.get();
            LOGGER.info("API_1_0_1 FL requesting from Merger  {}  total {}  seen {}", n, tot, rec);
        })
        .doOnCancel(() -> {
            LOGGER.info("API_1_0_1 FL cancel Merger");
        })
        .doOnNext(buf -> {
            long rec = totalSeenItems.addAndGet(1);
            long tot = totalRequestedItems.get();
            LOGGER.trace("API_1_0_1 FL item   total {}  seen {}", tot, rec);
        })
        .doOnTerminate(() -> {
            LOGGER.info("API_1_0_1 FL TERMINATED");
        });
    }

    <T> Flux<T> buildMerged(QueryParams qp, ServerHttpRequest req, TransformSupplier<T> transformSupplier) {
        final int nChannels = qp.channels.size();
        final AtomicLong oChannels = new AtomicLong();
        TokenSinker tsinker = new TokenSinker(splitNodes.size());
        Flux<Flux<T>> fcmf = Flux.fromIterable(qp.channels)
        .map(channelName -> {
            long channelIx = oChannels.getAndAdd(1);
            LOGGER.info("{}  buildMerged next channel {}", req.getId(), channelName);
            return Flux.fromIterable(splitNodes)
            .subscribeOn(clientsplitnodeiter)
            .filter(sn -> qp.splits == null || qp.splits.isEmpty() || qp.splits.contains(sn.split))
            .doOnNext(sn -> {
                LOGGER.debug("{}  buildMerged next split node  sn {} {}  sp {}", req.getId(), sn.host, sn.port, sn.split);
            })
            .zipWith(Flux.<Integer>create(sink -> {
                AtomicLong tState = new AtomicLong();
                sink.onRequest(reqno -> {
                    long stn = tState.getAndAdd(1);
                    if (stn < 0 || stn > splitNodes.size()) {
                        LOGGER.error("{}  Too many sub-requests {}", req.getId(), stn);
                        throw new RuntimeException("logic");
                    }
                    if (reqno != 1) {
                        throw new RuntimeException("bad request limit");
                    }
                    new Thread(() -> {
                        try {
                            int a = tsinker.queue.take();
                            sink.next(a);
                        }
                        catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }).start();
                });
                sink.onCancel(() -> {
                    LOGGER.info("Token flux cancelled");
                });
                sink.onDispose(() -> {
                    LOGGER.info("Dispose of token flux");
                });
            })
            .doOnDiscard(Integer.class, tok -> tsinker.putBack(tok))
            .limitRate(1)
            .publishOn(clientsplitnodeiter),
            1)
            .concatMap(tok -> {
                SplitNode sn = tok.getT1();
                Integer token = tok.getT2();
                LOGGER.debug("{}  buildMerged Query  {}  {}/{}  sn {} {}  sp {}  token {}", req.getId(), channelName, channelIx, nChannels, sn.host, sn.port, sn.split, token);
                String localURL = String.format("http://%s:%d/api/1.0.1/rawLocal", sn.host, sn.port);
                Query subq = new Query();
                subq.decompressOnServer = 0;
                subq.bufferSize = qp.bufferSize;
                subq.channels = List.of(channelName);
                Range range = new Range();
                range.startDate = qp.beginString;
                range.endDate = qp.endString;
                subq.range = range;
                subq.splits = List.of(sn.split);
                String js;
                try {
                    ObjectMapper mapper = new ObjectMapper(new JsonFactory());
                    js = mapper.writeValueAsString(subq);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }

                // Skip the first few bytes
                AtomicLong skipInResponse = new AtomicLong(4);

                /*
                Mono<Flux<Item>> m3 = Mono.just(1).map(x -> {
                    // provide at least the first bytes that we need to skip over
                    // and some more because client waits for that?
                    return Flux.just(defaultDataBufferFactory.wrap(new byte[]{'1', '2', '3', '4'}));
                })
                */

                Mono<Flux<Item>> m3 = springWebClientRequest(localURL, channelName, js, req)
                .map(clientResponse -> {
                    LOGGER.debug(logMarkerWebClientResponse, "WebClient  create body flux  channel {}  sn {} {}  sp {}", channelName, sn.host, sn.port, sn.split);
                    String remote_reqid = clientResponse.headers().header("x-daqbuffer-request-id").get(0);
                    return Tuples.of(remote_reqid, clientResponse.bodyToFlux(DataBuffer.class));
                })
                .<Flux<Item>>map(tup -> {
                    String remote_reqid = tup.getT1();
                    Flux<DataBuffer> flbuf = tup.getT2();
                    return flbuf.doOnSubscribe(s -> {
                        LOGGER.debug(logMarkerWebClientResponse, "WebClient Response  {}  {}  {}  SUBSCRIBE", channelName, sn.host, sn.split);
                    })
                    .doOnNext(buf -> {
                        int readable = buf.readableByteCount();
                        long skip = skipInResponse.get();
                        LOGGER.trace(logMarkerWebClientResponseItem, "WebClient Response  {}  {}  {}  NEXT   skip-A {} / {}", channelName, sn.host, sn.split, skip, readable);
                        if (skip > 0) {
                            if (skip > readable) {
                                skip = readable;
                            }
                            buf.readPosition(buf.readPosition() + (int) skip);
                            skipInResponse.getAndAdd(-skip);
                        }
                    })
                    .onErrorResume(e -> {
                        String requestStatusUrl = String.format("http://%s:%d/api/1.0.1/requestStatus/%s", sn.host, sn.port, remote_reqid);
                        return WebClient.builder()
                        .baseUrl(requestStatusUrl)
                        .build()
                        .get()
                        .exchange()
                        .doOnError(e2 -> {
                            LOGGER.error("WebClient requestStatus exchange doOnError {}", e2.toString());
                        })
                        .flatMapMany(x -> {
                            LOGGER.info("requestStatus  http status {}", x.statusCode());
                            return x.bodyToFlux(DataBuffer.class);
                        })
                        .reduce(qp.bufFac.allocateBuffer(), (a, x) -> {
                            a = a.ensureCapacity(a.readableByteCount() + x.writableByteCount());
                            a.write(x);
                            return a;
                        })
                        .flatMapMany(x -> {
                            String remoteBodyMsg = StandardCharsets.UTF_8.decode(x.asByteBuffer(x.readPosition(), x.readableByteCount())).toString();
                            LOGGER.warn("remote gave message {}", remoteBodyMsg);
                            synchronized (requestStatuses) {
                                RequestStatus st = requestStatuses.get(req.getId());
                                if (st == null) {
                                    st = new RequestStatus();
                                    requestStatuses.put(req.getId(), st);
                                }
                                if (st.errors == null) {
                                    st.errors = new ArrayList<>();
                                }
                                try {
                                    ObjectMapper om = new ObjectMapper();
                                    RequestStatus st2 = om.readValue(remoteBodyMsg, RequestStatus.class);
                                    st.errors.addAll(st2.errors);
                                }
                                catch (IOException e2) {
                                    LOGGER.error("can not parse json  {}   {}", remoteBodyMsg, e2.toString());
                                    RequestStatus.Error err = new RequestStatus.Error();
                                    err.msg = remoteBodyMsg;
                                    st.errors.add(err);
                                }
                            }
                            return Flux.error(new RuntimeException("subnode error"));
                        });
                    })
                    .doOnCancel(() -> {
                        LOGGER.info(logMarkerWebClientResponse, "WebClient Response  {}  {}  {}  CANCEL", channelName, sn.host, sn.split);
                    })
                    .doOnComplete(() -> {
                        LOGGER.debug(logMarkerWebClientResponse, "WebClient Response  {}  {}  {}  COMPLETE", channelName, sn.host, sn.split);
                        tsinker.putBack(token);
                    })
                    .doOnTerminate(() -> {
                        LOGGER.debug(logMarkerWebClientResponse, "WebClient Response  {}  {}  {}  TERMINATE", channelName, sn.host, sn.split);
                    })
                    .timeout(Duration.ofMillis(40000))
                    .onErrorMap(TimeoutException.class, e -> {
                        LOGGER.error("{}  Timeout1  channel {}  {} {}  {}", req.getId(), channelName, sn.host, sn.port, sn.split);
                        return new Timeout1();
                    })
                    .transform(fbuf2 -> EventBlobToV1MapTs.trans2(EventBlobToV1MapTs.Mock.NONE, fbuf2, String.format("__sn_%02d__buildMerged__%s", sn.split, channelName), channelName, qp.endNanos, qp.bufFac, qp.bufferSize))
                    .timeout(Duration.ofMillis(60000))
                    .onErrorMap(TimeoutException.class, e -> {
                        LOGGER.error("{}  Timeout2  channel {}  {} {}  {}", req.getId(), channelName, sn.host, sn.port, sn.split);
                        return new Timeout2();
                    })
                    .doOnComplete(() -> {
                        LOGGER.debug("after EventBlobToV1MapTs  {}  {}  COMPLETE", channelName, sn.host);
                    })
                    .doOnTerminate(() -> {
                        LOGGER.debug("after EventBlobToV1MapTs  {}  {}  TERMINATE", channelName, sn.host);
                    })
                    .transform(fl3 -> logFlux(String.format("merged_sn%02d_ts_flux_ts", sn.split), req, fl3));
                });
                return logMono(String.format("merged_sn%02d_ts_mono_sub", sn.split), req, m3);
            })
            .collectList()
            .<Flux<DataBuffer>>map(lfl -> pipeThroughMerger(lfl, channelName, qp))
            .flatMapMany(fl12 -> transformSupplier.trans3(fl12, channelName))
            .doOnSubscribe(s -> {
                LOGGER.debug("merged stream SUBSCRIBE  {}", channelName);
            })
            .doOnCancel(() -> {
                LOGGER.info("merged stream CANCEL     {}", channelName);
            })
            .doOnComplete(() -> {
                LOGGER.debug("merged stream COMPLETE   {}", channelName);
            })
            .doOnTerminate(() -> {
                LOGGER.debug("merged stream TERMINATE  {}", channelName);
            })
            .timeout(Duration.ofMillis(80000))
            .onErrorMap(TimeoutException.class, e -> {
                LOGGER.error("{}  Timeout3  channel {}", req.getId(), channelName);
                return new Timeout3();
            });
        });
        return fcmf
        .<T>concatMap(x -> x, 1);
    }

    static class TransMapQueryMerged implements TransformSupplier<DataBuffer> {
        QueryParams qp;
        TransMapQueryMerged(QueryParams qp) {
            this.qp = qp;
        }
        public Flux<DataBuffer> trans3(Flux<DataBuffer> fl, String channelName) {
            return EventBlobToV1Map.trans2(fl, channelName, qp.endNanos, qp.bufFac, qp.bufferSize, qp.decompressOnServer, qp.limitBytes)
            .map(res -> {
                if (res.buf == null) {
                    LOGGER.error("BAD: res.buf == null");
                    throw new RuntimeException("BAD: res.buf == null");
                }
                return res.buf;
            });
        }
    }

    static class TransMapQueryMergedFake implements TransformSupplier<DataBuffer> {
        TransMapQueryMergedFake(QueryParams qp) {}
        public Flux<DataBuffer> trans3(Flux<DataBuffer> fl, String channelName) {
            return fl;
        }
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedOctets(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        API_1_0_1.logHeaders(exchange);
        ServerHttpRequest req = exchange.getRequest();
        synchronized (requestStatuses) {
            RequestStatus stat = new RequestStatus();
            requestStatuses.put(req.getId(), stat);
        }
        if (!req.getHeaders().getAccept().contains(MediaType.APPLICATION_OCTET_STREAM)) {
            LOGGER.warn("{}  queryMerged  Client omits Accept: {}", req.getId(), MediaType.APPLICATION_OCTET_STREAM);
        }
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.info("can not parse request"))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(query, exchange.getResponse().bufferFactory(), bufferSize);
            LOGGER.info(String.format("%s  queryMerged  %s  %s  %s", req.getId(), qp.begin, qp.end, qp.channels));
            AtomicLong nbytes = new AtomicLong();
            return buildMerged(qp, req, new TransMapQueryMerged(qp))
            .doOnSubscribe(nreq1 -> {
                LOGGER.info("REQUEST FROM buildMerged nreq {}", nreq1);
            })
            .doOnNext(buf -> {
                if (buf == null) {
                    LOGGER.error("buf is null, can not add readable byte count");
                    throw new RuntimeException("logic");
                }
                LOGGER.trace(logMarkerQueryMergedItems, "{}  queryMerged  net emit  len {}", req.getId(), buf.readableByteCount());
                long h = nbytes.addAndGet(buf.readableByteCount());
                if (query.errorAfterBytes > 0 && h > query.errorAfterBytes) {
                    throw new RuntimeException("Byte limit reached");
                }
            })
            .doOnComplete(() -> {
                LOGGER.info("{}  queryMerged COMPLETE", req.getId());
            })
            .doOnTerminate(() -> {
                LOGGER.info("{}  queryMerged TERMINATE", req.getId());
            })
            .doOnError(e -> {
                LOGGER.info("{}  queryMerged ERROR", req.getId());
                String reqId = exchange.getRequest().getId();
                synchronized (requestStatuses) {
                    RequestStatus st = requestStatuses.get(reqId);
                    if (st == null) {
                        st = new RequestStatus();
                        requestStatuses.put(reqId, st);
                    }
                    if (st.errors == null) {
                        st.errors = new ArrayList<>();
                    }
                    RequestStatus.Error err = new RequestStatus.Error();
                    err.msg = String.format("%s", e.toString());
                    st.errors.add(err);
                }
            });
        });
        return logResponse("queryMerged", mret, req);
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedDebug(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        ServerHttpRequest req = exchange.getRequest();
        if (!req.getHeaders().getAccept().contains(MediaType.APPLICATION_OCTET_STREAM)) {
            LOGGER.warn("{}  queryMerged  Client omits Accept: {}", req.getId(), MediaType.APPLICATION_OCTET_STREAM);
        }
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.info("can not parse request"))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(query, exchange.getResponse().bufferFactory(), bufferSize);
            LOGGER.info(String.format("%s  queryMerged  %s  %s  %s", req.getId(), qp.begin, qp.end, qp.channels));
            return buildMerged(qp, req, new TransMapQueryMergedFake(qp))
            .doOnSubscribe(nreq1 -> {
                LOGGER.info("REQUEST FROM buildMerged nreq {}", nreq1);
            })
            .doOnNext(buf -> {
                if (buf == null) {
                    LOGGER.error("buf is null, can not add readable byte count");
                    throw new RuntimeException("logic");
                }
                LOGGER.trace(logMarkerQueryMergedItems, "{}  queryMerged  net emit  len {}", req.getId(), buf.readableByteCount());
            })
            .doOnComplete(() -> {
                LOGGER.info("{}  queryMerged COMPLETE", req.getId());
            })
            .doOnTerminate(() -> {
                LOGGER.info("{}  queryMerged TERMINATE", req.getId());
            });
        });
        return logResponse("queryMerged", mret, req);
    }

    static class MakeTrans2 implements MapFunctionFactory<DataBuffer> {
        @Override
        public Flux<DataBuffer> makeTrans(Flux<DataBuffer> fl, KeyspaceToDataParams kspp, int fileno) {
            return fl;
        }
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedLocal(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        API_1_0_1.logHeaders(exchange);
        ServerHttpRequest req = exchange.getRequest();
        synchronized (requestStatuses) {
            RequestStatus stat = new RequestStatus();
            requestStatuses.put(req.getId(), stat);
        }
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.info("can not parse request"))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(query, exchange.getResponse().bufferFactory(), bufferSize);
            LOGGER.info(String.format("%s  queryMergedLocal  %s  %s  %s", req.getId(), qp.begin, qp.end, qp.channels));
            Instant begin = Instant.parse(query.range.startDate);
            Instant end = Instant.parse(query.range.endDate);
            long endNanos = 1000000L * end.toEpochMilli();
            Flux<Mono<Flux<EventBlobMapResult>>> fcmf = Flux.fromIterable(query.channels)
            .map(channelName -> {
                Flux<Mono<Flux<Item>>> fmf = Flux.fromIterable(splitNodes)
                .filter(sn -> qp.splits == null || qp.splits.isEmpty() || qp.splits.contains(sn.split))
                .map(sn -> {
                    LOGGER.info("Local split {}", sn.split);
                    Query subq = new Query();
                    subq.decompressOnServer = 0;
                    subq.bufferSize = qp.bufferSize;
                    subq.channels = List.of(channelName);
                    subq.range = query.range;
                    subq.splits = List.of(sn.split);
                    Function<KeyspaceToDataParams, Mono<List<Flux<DataBuffer>>>> keyspaceToData = p -> {
                        return ChannelEventStream.dataFluxFromFiles(p, new MakeTrans2());
                    };
                    Flux<DataBuffer> fbuf = channelsToData(baseDirFinder, exchange.getRequest(), subq.channels, qp.begin, qp.end, subq.splits, qp.bufFac, qp.bufferSize, keyspaceToData)
                    .doOnDiscard(DataBuffer.class, DataBufferUtils::release);
                    Flux<Item> flItem = EventBlobToV1MapTs.trans2(EventBlobToV1MapTs.Mock.NONE, fbuf, String.format("__sn%02d__QML", sn.split), channelName, endNanos, qp.bufFac, qp.bufferSize);
                    return Mono.just(flItem);
                    /*
                    return logMono(String.format("merged_sn%02d_ts_mono_sub", sn.split), req, m3);
                    */
                });
                Flux<Mono<Flux<Item>>> fmf2 = logFlux("merged_ts_flux_subs", req, fmf);
                Mono<Flux<EventBlobMapResult>> fl4 = fmf2.concatMap(Function.identity(), 1)
                .map(fl -> fl.doOnDiscard(DataBuffer.class, DataBufferUtils::release))
                .collectList()
                .map(lfl -> Flux.from(new Merger(channelName, lfl, qp.bufFac, qp.bufferSize)))
                .map(flbuf -> {
                    return EventBlobToV1Map.trans2(flbuf, channelName, endNanos, qp.bufFac, qp.bufferSize, qp.decompressOnServer, qp.limitBytes)
                    .doOnNext(x2 -> {
                        if (x2.term) {
                            LOGGER.warn("EventBlobToV1Map reached TERM");
                        }
                    })
                    .takeWhile(x2 -> !x2.term);
                });
                return fl4;
            });
            Flux<Flux<EventBlobMapResult>> fl5 = fcmf.concatMap(x -> x, 1);
            Flux<EventBlobMapResult> fl6 = fl5.concatMap(x -> x, 1);
            Flux<DataBuffer> fl7 = fl6.map(x -> x.buf);
            return fl7;
        });
        return logResponse("queryMergedLocal", mret, req);
    }

    static class TransformSup3 implements TransformSupplier<MapJsonResult> {
        QueryParams qp;
        TransformSup3(QueryParams qp) {
            this.qp = qp;
        }
        public Flux<MapJsonResult> trans3(Flux<DataBuffer> fl, String channelName) {
            return EventBlobToJsonMap.trans2(fl, channelName, qp.endNanos, qp.bufFac, qp.bufferSize, qp.limitBytes);
        }
    }

    static class JgenState {
        boolean inChannel;
        void beOutOfChannel(JsonGenerator jgen) {
            if (inChannel) {
                inChannel = false;
                try {
                    jgen.writeEndArray();
                    jgen.writeEndObject();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedJson(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        ServerHttpRequest req = exchange.getRequest();
        if (!req.getHeaders().getAccept().contains(MediaType.APPLICATION_JSON)) {
            LOGGER.warn("{}  queryMerged  Client omits Accept: {}", req.getId(), MediaType.APPLICATION_JSON);
            //throw new RuntimeException("Incompatible Accept header");
        }
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.info("can not parse request"))
        .map(query -> {
            BinFind binFind;
            List<AggFunc> aggs;
            if (query.aggregation != null) {
                binFind = new BinFind(query.aggregation.nrOfBins, query.range);
                aggs = new ArrayList<>();
                for (String n : query.aggregation.aggregations) {
                    if (n.equals("sum")) {
                        aggs.add(new AggSum());
                    }
                    else if (n.equals("mean")) {
                        aggs.add(new AggMean());
                    }
                    else if (n.equals("min")) {
                        aggs.add(new AggMin());
                    }
                    else if (n.equals("max")) {
                        aggs.add(new AggMax());
                    }
                }
            }
            else {
                try {
                    LOGGER.warn("no aggregation in query: {}", new ObjectMapper().writeValueAsString(query));
                }
                catch (IOException e) {
                    LOGGER.warn("no aggregation in query but can not show exception");
                }
                binFind = null;
                aggs = null;
            }
            QueryParams qp = QueryParams.fromQuery(query, defaultDataBufferFactory, bufferSize);
            LOGGER.info(String.format("%s  queryMerged  %s  %s  %s", req.getId(), qp.begin, qp.end, qp.channels));
            JgenState jst = new JgenState();
            JsonGenerator jgen;
            JsonFactory jfac = new JsonFactory();
            OutputBuffer outbuf = new OutputBuffer(qp.bufFac);
            try {
                jgen = jfac.createGenerator(outbuf);
                // Sending the response as a plain array instead of wrapping the array of channels into an object-
                // member is specifically demanded.
                // https://jira.psi.ch/browse/CTRLIT-7984
                jgen.writeStartArray();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            AtomicLong binCurrent = new AtomicLong(-100);
            AtomicLong evCount = new AtomicLong();
            AtomicLong binTs = new AtomicLong();
            AtomicLong binPulse = new AtomicLong();
            return buildMerged(qp, req, new TransformSup3(qp))
            .map(res -> {
                //LOGGER.trace("MapJsonResult  {}", res.items.size());
                try {
                    for (MapJsonItem item : res.items) {
                        if (item instanceof MapJsonChannelStart) {
                            LOGGER.info("Channel Start");
                            MapJsonChannelStart ch = (MapJsonChannelStart) item;
                            jst.beOutOfChannel(jgen);
                            jst.inChannel = true;
                            jgen.writeStartObject();
                            jgen.writeStringField("name", ch.name);
                            jgen.writeFieldName("data");
                            jgen.writeStartArray();
                        }
                        else if (item instanceof MapJsonEvent) {
                            MapJsonEvent ev = (MapJsonEvent) item;
                            if (binFind != null) {
                                int bin = binFind.find(ev.ts);
                                LOGGER.warn("bin {}  binCurrent {}", bin, binCurrent.get());
                                if (binCurrent.get() == -100) {
                                    binCurrent.set(bin);
                                    binTs.set(ev.ts);
                                    binPulse.set(ev.pulse);
                                }
                                else if (bin != binCurrent.get()) {
                                    jgen.writeStartObject();
                                    jgen.writeFieldName("ts");
                                    jgen.writeStartObject();
                                    jgen.writeNumberField("sec", binTs.get() / 1000000000L);
                                    jgen.writeNumberField("ns", binTs.get() % 1000000000L);
                                    jgen.writeEndObject();
                                    jgen.writeNumberField("pulse", binPulse.get());
                                    jgen.writeObjectFieldStart("data");
                                    for (AggFunc f : aggs) {
                                        jgen.writeFieldName(f.name());
                                        f.result().serialize(jgen, new DefaultSerializerProvider.Impl());
                                    }
                                    jgen.writeEndObject();
                                    jgen.writeNumberField("eventCount", evCount.get());
                                    jgen.writeEndObject();
                                    binCurrent.set(bin);
                                    binTs.set(ev.ts);
                                    binPulse.set(ev.pulse);
                                    evCount.set(0);
                                    for (AggFunc f : aggs) {
                                        f.reset();
                                    }
                                }
                                evCount.getAndAdd(1);
                                for (AggFunc f : aggs) {
                                    f.sink(ev.data);
                                }
                            }
                            else {
                                jgen.writeStartObject();
                                jgen.writeFieldName("ts");
                                jgen.writeStartObject();
                                jgen.writeNumberField("sec", ev.ts / 1000000000L);
                                jgen.writeNumberField("ns", ev.ts % 1000000000L);
                                jgen.writeEndObject();
                                jgen.writeNumberField("pulse", ev.pulse);
                                jgen.writeFieldName("data");
                                ev.data.serialize(jgen, new DefaultSerializerProvider.Impl());
                                //jgen.writeObject(ev.data);
                                jgen.writeEndObject();
                            }
                        }
                        else {
                            throw new RuntimeException("logic");
                        }
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                res.release();
                return outbuf.getPending();
            })
            .concatWith(Mono.defer(() -> {
                try {
                    if (jst.inChannel) {
                        if (binFind != null) {
                            jgen.writeStartObject();
                            jgen.writeFieldName("ts");
                            jgen.writeStartObject();
                            jgen.writeNumberField("sec", binTs.get() / 1000000000L);
                            jgen.writeNumberField("ns", binTs.get() % 1000000000L);
                            jgen.writeEndObject();
                            jgen.writeNumberField("pulse", binPulse.get());
                            jgen.writeObjectFieldStart("data");
                            for (AggFunc f : aggs) {
                                jgen.writeFieldName(f.name());
                                f.result().serialize(jgen, new DefaultSerializerProvider.Impl());
                            }
                            jgen.writeEndObject();
                            jgen.writeNumberField("eventCount", evCount.get());
                            jgen.writeEndObject();
                        }
                    }
                    jst.beOutOfChannel(jgen);
                    jgen.writeEndArray();
                    jgen.close();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return Mono.just(outbuf.getPending());
            }))
            .concatMapIterable(Function.identity(), 1)
            .doOnNext(buf -> {
                totalBytesServed.getAndAdd(buf.readableByteCount());
            })
            .doOnTerminate(() -> {
                outbuf.release();
            });
        });
        return logMono("queryMergedJson", req, mret.map(x -> {
            return ResponseEntity.ok()
            .header("X-CanonicalHostname", canonicalHostname)
            .contentType(MediaType.APPLICATION_JSON)
            .body(x);
        }));
    }

    static <T> Mono<T> logMono(String name, ServerHttpRequest req, Mono<T> m) {
        if (true) return m;
        return m
        .doOnSuccess(x -> {
            LOGGER.info(String.format("%s  %s  success", req.getId(), name));
        })
        .doOnCancel(() -> {
            LOGGER.info(String.format("%s  %s  cancel", req.getId(), name));
        })
        .doOnError(e -> {
            LOGGER.info(String.format("%s  %s  error %s", req.getId(), name, e.toString()));
        })
        .doOnTerminate(() -> {
            LOGGER.info(String.format("%s  %s  terminate", req.getId(), name));
        });
    }

    static <T> Flux<T> logFlux(String name, ServerHttpRequest req, Flux<T> m) {
        if (true) return m;
        return m
        .doOnComplete(() -> {
            LOGGER.info(String.format("%s  %s  complete", req.getId(), name));
        })
        .doOnCancel(() -> {
            LOGGER.info(String.format("%s  %s  cancel", req.getId(), name));
        })
        .doOnError(e -> {
            LOGGER.info(String.format("%s  %s  error %s", req.getId(), name, e.toString()));
        })
        .doOnTerminate(() -> {
            LOGGER.info(String.format("%s  %s  terminate", req.getId(), name));
        });
    }

    <T> Mono<ResponseEntity<Flux<T>>> logResponse(String name, Mono<Flux<T>> m, ServerHttpRequest req) {
        return logMono(name, req, m.map(x -> {
            return ResponseEntity.ok()
            .header("X-CanonicalHostname", canonicalHostname)
            .header("x-daqbuffer-request-id", req.getId())
            .contentType(MediaType.APPLICATION_OCTET_STREAM)
            .body(x);
        }));
    }

    static Scheduler fs = Schedulers.newParallel("fs", 1);

    static <T> Flux<T> channelsToData(BaseDirFinderFormatV0 baseDirFinder, ServerHttpRequest req, List<String> channels, Instant begin, Instant end, List<Integer> splits, DataBufferFactory bufFac, int bufferSize, Function<KeyspaceToDataParams, Mono<List<Flux<T>>>> keyspaceToData) {
        Flux<T> ret = Flux.fromIterable(channels)
        .subscribeOn(fs)
        .concatMap(channelName -> {
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
            .concatMap(ks -> keyspaceToData.apply(new KeyspaceToDataParams(ks, begin, end, bufFac, bufferSize, splits, req)), 1)
            .concatMap(x -> {
                if (x.size() <= 0) {
                    throw new RuntimeException("logic");
                }
                if (x.size() > 1) {
                    throw new RuntimeException("not yet supported in local query");
                }
                return x.get(0);
            }, 1);
            return bulk;
        }, 1);
        Flux<T> ret2 = logFlux("channelsToData", req, ret);
        return ret2;
    }

}
