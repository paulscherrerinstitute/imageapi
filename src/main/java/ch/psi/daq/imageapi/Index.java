package ch.psi.daq.imageapi;

import com.google.common.io.BaseEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.ByteBufFlux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class Index {
    static Logger LOGGER = LoggerFactory.getLogger(Index.class);
    static final int N = 16;

    static class FindResult {
        int i;
        long k;
        long v;
        static FindResult none() {
            FindResult ret = new FindResult();
            ret.i = -1;
            ret.k = -1;
            ret.v = 0;
            return ret;
        }
        static FindResult at(byte[] a, int i) {
            FindResult ret = new FindResult();
            ret.i = i;
            ret.k = keyLongAt(a, i);
            ret.v = valueLongAt(a, i);
            return ret;
        }
        public boolean isSome() {
            return k > 0;
        }
        @Override
        public String toString() {
            if (isSome()) {
                return String.format("FindResult { k: %d, v: %d }", k, v);
            }
            return "FindResult { None }";
        }
    }

    static FindResult findGEByLong(long tgt, byte[] a) {
        byte[] buf2 = new byte[8];
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(tgt);
        buf.flip();
        buf.get(buf2);
        return findGE(buf2, a);
    }

    static FindResult findGE(byte[] tgt, byte[] a) {
        if (a.length % N != 0) {
            throw new RuntimeException("findGE unexpected length");
        }
        int n = a.length;
        if (n < N) {
            return FindResult.none();
        }
        int j = 0;
        int k = n - N;
        //show(a, j);
        //show(a, k);
        if (cmp(a, j, tgt, 0) >= 0) {
            return FindResult.at(a, 0);
        }
        if (cmp(a, k, tgt, 0) < 0) {
            return FindResult.none();
        }
        while (true) {
            if (k - j < 2*N) {
                return FindResult.at(a, k);
            }
            int m = ((k + j) >> 1) & 0xfffffff0;
            //show(a, m);
            if (cmp(a, m, tgt, 0) < 0) {
                j = m;
            }
            else {
                k = m;
            }
        }
    }

    static int cmp(byte[] ba, int ia, byte[] bb, int ib) {
        for (int i = 0; i < 8; i+=1) {
            int ea = 0xff & ba[ia+i];
            int eb = 0xff & bb[ib+i];
            if (ea < eb) { return -1; }
            if (ea > eb) { return +1; }
        }
        return 0;
    }

    static String show(byte[] buf, int a) {
        return BaseEncoding.base16().lowerCase().encode(buf, a, 8);
    }

    static Mono<byte[]> openIndex(Path indexPath) {
        if (true) {
            return Mono.fromCallable(() -> {
                try {
                    byte[] b1 = Files.readAllBytes(indexPath);
                    int n = b1.length;
                    //LOGGER.info(String.format("reading index 2nd  n: %d  %s", n, indexPath));
                    if (n < 2) {
                        throw new RuntimeException(String.format("Index file is too small n: %d  %s", n, indexPath));
                    }
                    if ((n-2) % N != 0) {
                        throw new RuntimeException(String.format("unexpected index file content  n: %d  %s", n, indexPath));
                    }
                    byte[] b2 = Arrays.copyOfRange(b1, 2, n);
                    int n2 = b2.length;
                    if (n2 < 2) {
                        throw new RuntimeException(String.format("Index file is too small n: %d  n2: %d  %s", n, n2, indexPath));
                    }
                    if (n2 % N != 0) {
                        throw new RuntimeException(String.format("unexpected index file content  n: %d  n2: %d  %s", n, n2, indexPath));
                    }
                    return b2;
                }
                catch (IOException e) {
                    LOGGER.error("IOException readAllBytes", e);
                }
                catch (OutOfMemoryError e) {
                    LOGGER.error("OutOfMemoryError readAllBytes", e);
                }
                catch (SecurityException e) {
                    LOGGER.error("SecurityException readAllBytes", e);
                }
                throw new RuntimeException("can not read index");
            })
            .subscribeOn(Schedulers.parallel());
        }
        return ByteBufFlux.fromPath(indexPath)
        .aggregate()
        .map(buf -> {
            long nLong = buf.readableBytes();
            if (nLong < 2) {
                throw new RuntimeException(String.format("Index file is too small %s", indexPath));
            }
            if (nLong >= Integer.MAX_VALUE) {
                throw new RuntimeException(String.format("nLong >= Integer.MAX_VALUE  %s", indexPath));
            }
            int n = (int) nLong;
            if ((n-2) % N != 0) {
                throw new RuntimeException(String.format("unexpected index file content  %s", indexPath));
            }
            n = n - 2;
            byte[] a = new byte[n];
            buf.getBytes(2, a);
            return a;
        });
    }

    static long valueLongAt(byte[] a, int i) {
        return ByteBuffer.wrap(a).getLong(8+i);
    }

    static long keyLongAt(byte[] a, int i) {
        return ByteBuffer.wrap(a).getLong(i);
    }

}
