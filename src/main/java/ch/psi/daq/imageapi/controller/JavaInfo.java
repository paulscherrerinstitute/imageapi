package ch.psi.daq.imageapi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@RestController
public class JavaInfo {
    @Autowired
    API_1_0_1 api101;

    public static class MemStdUsage {
        public long init;
        public long committed;
        public long used;
        public long max;
        public MemStdUsage(MemoryUsage mu) {
            if (mu == null) {
                init = -1;
                committed = 1;
                used = -1;
                max = -1;
            }
            else {
                init = mu.getInit();
                committed = mu.getCommitted();
                used = mu.getUsed();
                max = mu.getUsed();
            }
        }
    }

    public static class MemStd {
        public MemStdUsage heap;
        public MemStdUsage nonheap;
        public int pending;
        public MemStd(MemoryMXBean b) {
            heap = new MemStdUsage(b.getHeapMemoryUsage());
            nonheap = new MemStdUsage(b.getNonHeapMemoryUsage());
            pending = b.getObjectPendingFinalizationCount();
        }
    }

    public static class MemPool {
        public String name;
        public MemStdUsage usage;
        public MemStdUsage collection;
        public int pending;
        public MemPool(MemoryPoolMXBean b) {
            name = b.getName();
            usage = new MemStdUsage(b.getUsage());
            collection = new MemStdUsage(b.getCollectionUsage());
        }
    }

    public static class BufferPool {
        public String name;
        public long used;
        public long count;
        public long totalCapacity;
        public BufferPool(BufferPoolMXBean b) {
            name = b.getName();
            used = b.getMemoryUsed();
            count = b.getCount();
            totalCapacity = b.getTotalCapacity();
        }
    }

    public static class Stats {
        public long served;
        public List<MemStd> memStd = new ArrayList<>();
        public List<MemPool> memPool = new ArrayList<>();
        public List<BufferPool> bufferPools = new ArrayList<>();
    }

    @GetMapping(path = "/stats", produces = MediaType.APPLICATION_JSON_VALUE)
    public Stats stats() {
        Stats ret = new Stats();
        ret.served = api101.getTotalBytesServed();
        for (BufferPoolMXBean bean : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
            ret.bufferPools.add(new BufferPool(bean));
        }
        ret.memStd.add(new MemStd(ManagementFactory.getMemoryMXBean()));
        for (MemoryMXBean bean : ManagementFactory.getPlatformMXBeans(MemoryMXBean.class)) {
            ret.memStd.add(new MemStd(bean));
        }
        for (MemoryPoolMXBean bean : ManagementFactory.getPlatformMXBeans(MemoryPoolMXBean.class)) {
            ret.memPool.add(new MemPool(bean));
        }
        return ret;
    }

    @GetMapping(path = "/static/opt/ret/{pathString}")
    public ResponseEntity<DataBuffer> staticOptRet(@PathVariable String pathString) throws IOException {
        InputStream inp = null;
        String html = null;
        //html = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(inp.readAllBytes())).toString();
        DataBufferFactory bufFac = new DefaultDataBufferFactory();
        html = String.format("%s", pathString);
        if (pathString.matches(".*(//|\\.\\.).*")) {
            ByteBuffer buf1 = StandardCharsets.UTF_8.encode("invalid path: " + pathString);
            DataBuffer buf2 = new DefaultDataBufferFactory().wrap(buf1);
            return ResponseEntity.badRequest().body(buf2);
        }
        Path path = Path.of("/opt/retrieval").resolve(pathString);
        if (!Files.isRegularFile(path)) {
            ByteBuffer buf1 = StandardCharsets.UTF_8.encode("not a file: " + pathString);
            DataBuffer buf2 = bufFac.wrap(buf1);
            return ResponseEntity.badRequest().body(buf2);
        }
        return ResponseEntity.ok(bufFac.wrap(Files.readAllBytes(path)));
    }

}
