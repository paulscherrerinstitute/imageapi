package ch.psi.daq.imageapi.finder;

import ch.psi.daq.imageapi.CacheLRU;
import ch.psi.daq.imageapi.config.ChannelConfig;
import ch.psi.daq.imageapi.Microseconds;
import ch.psi.daq.imageapi.config.ChannelConfigEntry;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBufferFactory;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BaseDirScanner {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger("BaseDirScanner");
    final CacheLRU<String, ScanDataFilesResult> cacheForChannel = new CacheLRU<>(1<<15);
    final Thread tix;
    final Map<Channel, CompletableFuture<ScanDataFilesResult>> inProgress = new TreeMap<>();
    List<String> keyspaces = List.of("2", "3", "4");
    Pattern patternTimeBin = Pattern.compile("[0-9]{19}");
    Pattern patternSplit = Pattern.compile("[0-9]{10}");
    Pattern patternDatafile = Pattern.compile("([0-9]{19})_([0-9]{5})_Data");

    public Mono<ScanDataFilesResult> getChannelScan(Channel channel, long beginMs, long endMs, List<Integer> splits, DataBufferFactory bufFac) {
        synchronized (inProgress) {
            // TODO can not cache the partial scan result, it will shadow the next!
            // should actually use a different type for the partial result.
            if (false && inProgress.containsKey(channel)) {
                //LOGGER.info("++++++++++++++++++++++++++  Scan for {} already running", channel);
                Mono<ScanDataFilesResult> m2 = Mono.fromFuture(inProgress.get(channel));
                return m2;
            }
            else {
                LOGGER.info("New scan for {}", channel);
                inProgress.put(channel, scanDataFiles2(channel, beginMs, endMs, splits, bufFac).toFuture());
                return Mono.fromFuture(inProgress.get(channel));
            }
        }
    }

    public Mono<ScanDataFilesResult> scanDataFiles2(Channel channel, long beginMs, long endMs, List<Integer> splits, DataBufferFactory bufFac) {
        LOGGER.debug("beginMs: {}  endMs: {}", beginMs, endMs);
        return BaseDirFinderFormatV0.channelConfig(channel, bufFac)
        .flatMap(x -> {
            if (x.isPresent()) {
                ChannelConfig conf = x.get();
                ScanDataFilesResult res = new ScanDataFilesResult();
                res.channelName = channel.name;
                res.keyspaces = new ArrayList<>();
                if (conf.entries.size() <= 0) {
                    LOGGER.warn("conf has no entries");
                }
                else {
                    // Find the first config entry before or at beginMs
                    // Use that keyspace from there...
                    for (int i1 = 0; i1 < conf.entries.size(); i1 += 1) {
                        ChannelConfigEntry e1 = conf.entries.get(i1);
                        LOGGER.info("ChannelConfigEntry e1  ix {}  ts {}  bs {}  bin {}", i1, e1.ts / 1000000, e1.bs, e1.ts / 1000000 / e1.bs);
                        ChannelConfigEntry e2 = null;
                        long toMs = endMs;
                        if (i1 < conf.entries.size() - 1) {
                            e2 = conf.entries.get(i1 + 1);
                            toMs = Math.min(toMs, e2.ts / 1000000);
                            LOGGER.info("ChannelConfigEntry e2  ix {}  ts {}  bs {}  bin {}", i1 + 1, e2.ts / 1000000, e2.bs, e2.ts / 1000000 / e2.bs);
                            if (e2.ts / 1000000 <= beginMs) {
                                LOGGER.info("DISREGARD AND CONTINUE");
                                continue;
                            }
                        }
                        LOGGER.debug("toMs: {}", toMs);
                        if (res.keyspaces.size() == 0) {
                            KeyspaceOrder2 ksp = new KeyspaceOrder2(String.format("%d", e1.ks));
                            ksp.channel = channel;
                            if (splits.isEmpty()) {
                                ksp.splits = new ArrayList<>();
                                for (int spi = 0; spi < e1.sc; spi += 1) {
                                    Split s = new Split(spi);
                                    s.timeBins = new ArrayList<>();
                                    ksp.splits.add(s);
                                }
                            }
                            else {
                                ksp.splits = splits.stream().map(sp -> {
                                    Split s = new Split(sp);
                                    s.timeBins = new ArrayList<>();
                                    return s;
                                }).collect(Collectors.toList());
                            }
                            res.keyspaces.add(ksp);
                        }
                        else {
                            if (!res.keyspaces.get(0).ksp.equals(String.format("%d", e1.ks))) {
                                //LOGGER.warn("!res.keyspaces.get(0).ksp.equals  i1: {}  {}  vs  {}", i1, res.keyspaces.get(0).ksp, e1.ks);
                                //throw new RuntimeException("logic");
                            }
                        }
                        LOGGER.debug("\nbeginMs: {}\ne1.ts:   {}\ntoMs:    {}", beginMs, e1.ts / 1000000, toMs);
                        long tsMs = (Math.max(beginMs, e1.ts / 1000000) / e1.bs) * e1.bs;
                        for (; tsMs < toMs; tsMs += e1.bs) {
                            long bin = tsMs / e1.bs;
                            LOGGER.debug("add file  tsMs: {}  bin: {}  next tsMs: {}", tsMs, bin, tsMs + e1.bs);
                            for (KeyspaceOrder2 ksp2 : res.keyspaces) {
                                LOGGER.info("Look at ksp2  ks {}  splits {}", ksp2.ksp, ksp2.splits);
                                for (Split split : ksp2.splits) {
                                    int dummy = 0;
                                    String fn = String.format("%s/%s_%s/byTime/%s/%019d/%010d/%019d_%05d_Data_Index", channel.base.baseDir, channel.base.baseKeyspaceName, ksp2.ksp, channel.name, bin, split.split, e1.bs, dummy);
                                    boolean hasIndex = Files.isReadable(Path.of(fn));
                                    TimeBin2 tb = new TimeBin2(bin, e1.bs, hasIndex);
                                    boolean alreadyThere = false;
                                    for (TimeBin2 tb2 : split.timeBins) {
                                        if (tb2.timeBin == tb.timeBin && tb2.binSize == tb.binSize) {
                                            alreadyThere = true;
                                            break;
                                        }
                                    }
                                    if (!alreadyThere) {
                                        split.timeBins.add(tb);
                                    }
                                }
                            }
                        }
                    }
                }
                //if (true) throw new RuntimeException("great, there is config, but still need to check for the files...");
                LOGGER.info("Summary  ks size {}  ksp {}  tb size {}", res.keyspaces.size(), res.keyspaces.get(0).ksp, res.keyspaces.get(0).splits.get(0).timeBins.size());
                return Mono.just(res);
            }
            else {
                LOGGER.warn("channel configuration not found  {}", channel);
                return scanDataFiles3(channel);
            }
        });
    }

    public Mono<ScanDataFilesResult> scanDataFiles3(Channel channel) {
        Mono<ScanDataFilesResult> mfind = Mono.fromCallable(() -> {
            LOGGER.warn("===========================================   Scan {}", channel);
            long tsScanBegin = System.nanoTime();
            List<KeyspaceOrder1> foundKeyspaces = keyspaces.stream()
            .map(x -> Tuples.of(x, Path.of(String.format("%s/%s/%s_%s/byTime/%s", channel.base.baseDir, channel.base.baseKeyspaceName, channel.base.baseKeyspaceName, x, channel.name))))
            .map(x -> {
                try {
                    String ksstr = x.getT1();
                    Path pathChannel = x.getT2();
                    List<Path> timeBinPaths;
                    try (Stream<Path> stream = Files.list(pathChannel)) {
                        timeBinPaths = stream.collect(Collectors.toList());
                    }
                    List<BaseDirFinderFormatV0.TimeBin> timeBins = timeBinPaths.stream().sequential()
                    .map(x2 -> {
                        String dirString = x2.getFileName().toString();
                        if (patternTimeBin.matcher(dirString).matches()) {
                            long bin = Long.parseLong(dirString);
                            return Optional.of(bin);
                        }
                        else {
                            return Optional.<Long>empty();
                        }
                    })
                    .filter(Optional::isPresent).map(Optional::get)
                    .map(timeBin -> {
                        try {
                            List<Path> splitPaths;
                            try (Stream<Path> stream = Files.list(pathChannel.resolve(String.format("%019d", timeBin)))) {
                                splitPaths = stream.collect(Collectors.toList());
                            }
                            List<BaseDirFinderFormatV0.SplitBinsize> listOfSplits = splitPaths.stream().sequential()
                            .flatMap(de -> {
                                String s = de.getFileName().toString();
                                if (patternSplit.matcher(s).matches()) {
                                    Long split = Long.parseLong(s);
                                    List<Path> datafilePaths;
                                    try {
                                        try (Stream<Path> stream = Files.list(pathChannel.resolve(String.format("%019d", timeBin)).resolve(String.format("%010d", split)))) {
                                            datafilePaths = stream.collect(Collectors.toList());
                                        }
                                        return datafilePaths.stream().sequential()
                                        .map(datafilename -> patternDatafile.matcher(datafilename.getFileName().toString()))
                                        .filter(Matcher::matches)
                                        .map(m -> {
                                            long bs = Long.parseLong(m.group(1));
                                            boolean ex = Files.exists(pathChannel.resolve(String.format("%019d", timeBin)).resolve(String.format("%010d", split)).resolve(String.format("%019d_%05d_Data_Index", bs, 0)));
                                            return BaseDirFinderFormatV0.SplitBinsize.create(split, bs, ex);
                                        });
                                    }
                                    catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                                else {
                                    return Stream.empty();
                                }
                            })
                            .collect(Collectors.toList());
                            BaseDirFinderFormatV0.TimeBin s1 = new BaseDirFinderFormatV0.TimeBin(timeBin);
                            s1.splits = listOfSplits;
                            return s1;
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList());
                    KeyspaceOrder1 ks = new KeyspaceOrder1(ksstr);
                    ks.timeBins = timeBins;
                    return Optional.of(ks);
                }
                catch (IOException e) {
                    return Optional.<KeyspaceOrder1>empty();
                }
            })
            .filter(Optional::isPresent).map(Optional::get)
            .collect(Collectors.toList());
            List<KeyspaceOrder2> keyspaces2 = foundKeyspaces.stream()
            .map(ks1 -> {
                Map<Long, Split> m23 = ks1.timeBins.stream().flatMap(x -> x.splits.stream().map(sp -> sp.split)).collect(Collectors.toSet()).stream()
                .map(s -> Tuples.of(s, new Split(s.intValue())))
                .collect(Collectors.toMap(Tuple2::getT1, Tuple2::getT2));
                ks1.timeBins.forEach(tb -> tb.splits.forEach(sp -> m23.get(sp.split).timeBins.add(new TimeBin2(tb.timeBin, sp.binsize, sp.hasIndex))));
                KeyspaceOrder2 ks2 = new KeyspaceOrder2(ks1.ksp);
                ks2.splits = new ArrayList<>(m23.values());
                ks2.channel = channel;
                return ks2;
            })
            .collect(Collectors.toList());
            ScanDataFilesResult ret = new ScanDataFilesResult();
            ret.channelName = channel.name;
            ret.keyspaces = keyspaces2;
            ret.duration = Microseconds.fromNanos(System.nanoTime() - tsScanBegin);
            return ret;
        })
        .doOnSubscribe(x -> LOGGER.info("*(*(*((*   Scanner subcribed for {}", channel));
        return mfind;
    }


    public Optional<ScanDataFilesResult> getNewer(String channel, Instant age) {
        synchronized (cacheForChannel) {
            Optional<ScanDataFilesResult> res = cacheForChannel.getYounger(channel, Instant.now().toEpochMilli() - age.toEpochMilli());
            if (res.isPresent()) {
                ScanDataFilesResult res2 = res.get();
                return Optional.of(res2);
            }
            else {
                return Optional.empty();
            }
        }
    }

    public BaseDirScanner() {
        tix = new Thread();
        tix.start();
    }

    void indexAll() {
    }

    void index(String baseDir, String baseKeyspaceName) {
        //Path.of(String.format("%s/%s/%s_%s/byTime/%s", baseDir, baseKeyspaceName, baseKeyspaceName, x, channel))
        Instant timeBegin = Instant.now();
        if (baseDir != null && baseKeyspaceName != null) {
            KeyspaceOrder2 ks = new KeyspaceOrder2("2");
            //ks.baseDir = baseDir;
            //ks.baseKeyspaceName = baseKeyspaceName;
            indexKeyspace(ks);
        }
        Instant timeEnd = Instant.now();
    }

    void indexKeyspace(KeyspaceOrder2 ks) {
        BaseDir base = ks.channel.base;
        Path path1 = Path.of(String.format("%s/%s/%s_%s/byTime", base.baseDir, base.baseKeyspaceName, base.baseKeyspaceName, ks.ksp));
        DirectoryStream<Path> stream1 = null;
        try {
            stream1 = Files.newDirectoryStream(path1);
            Iterator<Path> it1 = stream1.iterator();
            long n1 = 0;
            while (it1.hasNext()) {
                Path p1 = it1.next();
                LOGGER.info("next: {}", p1);
                // TODO scanDataFiles(p1.getFileName().toString()).block(Duration.ofMillis(20000));
                n1 += 1;
                if (n1 > 10) {
                    break;
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            try {
                if (stream1 != null) {
                    stream1.close();
                }
            }
            catch (IOException e) {
                LOGGER.error("IOException during unwind");
            }
        }
    }

}
