package ch.psi.daq.imageapi.controller;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.lang.management.*;
import java.util.ArrayList;
import java.util.List;

@RestController
public class Welcome {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger("Welcome");
    @Autowired
    ResourceLoader loader;
    @Value("${imageapi.staticFilesLocal:false}")
    boolean staticFilesLocal;
    @Autowired
    API_1_0_1 api101;

    @GetMapping(path = "/s/{fname}")
    public Resource staticFile(@PathVariable String fname) {
        if (staticFilesLocal) {
            return loader.getResource("file:src/main/resources/html/" + fname);
        }
        else {
            return loader.getResource("classpath:html/" + fname);
        }
    }

    @GetMapping(path = "/")
    public Resource index() {
        return staticFile("index.html");
    }

    public class MemStdUsage {
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

    public class MemStd {
        public MemStdUsage heap;
        public MemStdUsage nonheap;
        public int pending;
        public MemStd(MemoryMXBean b) {
            heap = new MemStdUsage(b.getHeapMemoryUsage());
            nonheap = new MemStdUsage(b.getNonHeapMemoryUsage());
            pending = b.getObjectPendingFinalizationCount();
        }
    }

    public class MemPool {
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

    public class BufferPool {
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

    public class Stats {
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

}
