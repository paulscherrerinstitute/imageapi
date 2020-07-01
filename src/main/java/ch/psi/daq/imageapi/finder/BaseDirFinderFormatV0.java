package ch.psi.daq.imageapi.finder;

import ch.psi.daq.imageapi.config.ChannelConfig;
import ch.psi.daq.imageapi.config.ChannelConfigEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Implements traversing the original version 0 directory hierarchy as currently used
 * by databuffer, imagebuffer, ...
 */
public class BaseDirFinderFormatV0 {
    static Logger LOGGER = LoggerFactory.getLogger(BaseDirFinderFormatV0.class);
    static BaseDirScanner scanner = new BaseDirScanner();
    String baseDir;
    String baseKeyspaceName;

    public BaseDirFinderFormatV0(String baseDir, String baseKeyspaceName) {
        //LOGGER.info("New finder for {} in {}", baseKeyspaceName, baseDir);
        this.baseDir = baseDir;
        this.baseKeyspaceName = baseKeyspaceName;
    }

    public static class TimeBin {
        TimeBin(long timeBin) {
            this.timeBin = timeBin;
        }
        public long timeBin;
        public List<SplitBinsize> splits;
    }

    public static class SplitBinsize {
        public long split;
        public long binsize;
        public boolean hasIndex = false;
        static SplitBinsize create(long split, long binsize, boolean hasIndex) {
            SplitBinsize ret = new SplitBinsize();
            ret.split = split;
            ret.binsize = binsize;
            ret.hasIndex = hasIndex;
            return ret;
        }
    }

    public static class MatchingDataFilesResult {
        public String channelName;
        public Instant begin;
        public Instant end;
        public List<KeyspaceOrder2> keyspaces;
    }

    public Mono<MatchingDataFilesResult> findMatchingDataFiles(String channelName, Instant begin, Instant end, List<Integer> splits, DataBufferFactory bufFac) {
        long beginMs = begin.toEpochMilli();
        long endMs = end.toEpochMilli();
        Channel channel = new Channel();
        channel.base = new BaseDir();
        channel.base.baseDir = baseDir;
        channel.base.baseKeyspaceName = baseKeyspaceName;
        channel.name = channelName;
        return scanDataFiles(channel, beginMs, endMs, splits, bufFac)
        .map(res -> {
            List<KeyspaceOrder2> lKsp = res.keyspaces.stream()
            .map(ks -> {
                List<Split> lSp = ks.splits.stream()
                .map(sp -> {
                    List<TimeBin2> lTb = sp.timeBins.stream()
                    .peek(tb -> {
                        //LOGGER.info(String.format("candidate: channel: %s  split: %d  bin: %d  binSize: %d", channel, sp.split, tb.timeBin, tb.binSize));
                    })
                    .filter(tb -> {
                        long beg = tb.timeBin * tb.binSize;
                        boolean pr = beg < endMs && (beg + tb.binSize > beginMs);
                        return pr;
                    })
                    .peek(tb -> {
                        LOGGER.info(String.format("chosen:    channel: %s  split: %d  bin: %d  binSize: %d", channel, sp.split, tb.timeBin, tb.binSize));
                    })
                    .collect(Collectors.toList());
                    Split sp2 = new Split(sp.split);
                    sp2.timeBins = lTb;
                    return sp2;
                })
                .filter(x -> x.timeBins.size() > 0)
                .collect(Collectors.toList());
                KeyspaceOrder2 ret = new KeyspaceOrder2(ks.ksp);
                ret.splits = lSp;
                ret.channel = ks.channel;
                return ret;
            })
            .filter(x -> x.splits.size() > 0)
            .collect(Collectors.toList());
            if (lKsp.size() > 1) {
                throw new RuntimeException("Channel must be only in one keyspace");
            }
            MatchingDataFilesResult ret = new MatchingDataFilesResult();
            ret.channelName = res.channelName;
            ret.begin = begin;
            ret.end = end;
            ret.keyspaces = lKsp;
            return ret;
        });
    }

    public Mono<ScanDataFilesResult> scanDataFiles(Channel channel, long beginMs, long endMs, List<Integer> splits, DataBufferFactory bufFac) {
        return scanner.getChannelScan(channel, beginMs, endMs, splits, bufFac);
    }

    public static Mono<Optional<ChannelConfig>> channelConfig(Channel channel, DataBufferFactory bufFac) {
        String fname = String.format("%s/config/%s/latest/00000_Config", channel.base.baseDir, channel.name);
        Path path = Path.of(fname);
        Flux<DataBuffer> fb = DataBufferUtils.readByteChannel(() -> {
            try {
                LOGGER.info("try open {}", path);
                return Files.newByteChannel(path);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, bufFac, 512 * 1024);
        return DataBufferUtils.join(fb, 1<<21)
        .map(Optional::of)
        .onErrorReturn(Optional.empty())
        .map(x -> {
            if (x.isEmpty()) {
                LOGGER.error("Could not read config for {}", path);
                throw new RuntimeException("I/O");
            }
            DataBuffer buf = x.get();
            ByteBuffer bbuf = buf.asByteBuffer(buf.readPosition(), buf.readableByteCount());
            if (bbuf.getShort() != 0) {
                throw new RuntimeException("logic");
            }
            int channelNameLength1 = bbuf.getInt();
            if (channelNameLength1 < 8) {
                LOGGER.info("channel name bad");
                throw new RuntimeException("logic");
            }
            channelNameLength1 -= 8;
            StandardCharsets.UTF_8.decode(bbuf.slice().limit(channelNameLength1));
            bbuf.position(bbuf.position() + channelNameLength1);
            if (bbuf.getInt() != channelNameLength1 + 8) {
                throw new RuntimeException("logic");
            }
            ChannelConfig conf = new ChannelConfig();
            while (bbuf.remaining() > 0) {
                int p1 = bbuf.position();
                int len1 = bbuf.getInt();
                if (len1 < 32 || len1 > 1024) {
                    throw new RuntimeException("logic");
                }
                if (bbuf.remaining() < len1 - Integer.BYTES) {
                    throw new RuntimeException("logic");
                }
                long ts = bbuf.getLong();
                long pulse = bbuf.getLong();
                int ks = bbuf.getInt();
                long bs = bbuf.getLong();
                int sc = bbuf.getInt();
                LOGGER.debug(String.format("found meta  ts: %d  ks: %d  bs: %d  sc: %d", ts, ks, bs, sc));
                ChannelConfigEntry e = new ChannelConfigEntry();
                e.ts = ts;
                e.ks = ks;
                e.bs = bs;
                e.sc = sc;
                conf.entries.add(e);
                bbuf.position(p1 + len1);
            }
            DataBufferUtils.release(buf);
            return Optional.of(conf);
        })
        //.onErrorReturn(Optional.empty())
        ;
    }

}
