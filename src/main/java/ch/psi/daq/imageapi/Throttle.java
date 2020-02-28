package ch.psi.daq.imageapi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

class Throttle implements Runnable {
    static Logger LOGGER = LoggerFactory.getLogger(Throttle.class);
    Thread thread;
    AtomicLong now = new AtomicLong(-1);
    AtomicLong count1 = new AtomicLong(0);
    static final int SLEEP_MS = 50;
    static final int BYTE_MS = 350 * 1024 * 1024 / 1000;
    static final int K1 = 100;

    Throttle() {
        LOGGER.info("Starting throttle");
        thread = new Thread(this, "ticker");
        thread.start();
    }

    Mono<DataBuffer> map(DataBuffer buf) {
        int nbuf = buf.readableByteCount();
        long count1l = count1.addAndGet(nbuf);
        if (count1l < K1 * BYTE_MS) {
            long dt = 10 * nbuf / BYTE_MS / 14;
            if (false) {
                LOGGER.info(String.format(".  dt: %10d  count1: %10d", dt, count1l/1024));
            }
            return Mono.just(buf).delayElement(Duration.ofMillis(dt));
        }
        else {
            long dt = (count1l - K1 * BYTE_MS) / BYTE_MS;
            if (false) {
                LOGGER.info(String.format(".  dt: %10d  count1: %10d", dt, count1l/1024));
            }
            return Mono.just(buf).delayElement(Duration.ofMillis(dt));
        }
    }

    public void run() {
        while (true) {
            try {
                long now2 = now();
                long now1 = now.getAndSet(now2);
                long count1l = count1.get();
                if (count1l > 0) count1.addAndGet(Math.max(-BYTE_MS * ((now2-now1)/1000000), -count1l));
                if (false) {
                    LOGGER.info(String.format("count1:  %10d", count1.get() / 1024));
                }
                Thread.sleep(SLEEP_MS);
            }
            catch (InterruptedException e) {
            }
        }
    }

    static long now() {
        return System.nanoTime();
    }

}
