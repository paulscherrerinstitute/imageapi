package ch.psi.daq.imageapi3;

import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Index {
    static final int N = 16;

    static class FindResult {
        long k;
        long v;
        static FindResult none() {
            FindResult ret = new FindResult();
            ret.k = -1;
            ret.v = 0;
            return ret;
        }
        static FindResult at(byte[] a, int i) {
            FindResult ret = new FindResult();
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
        buf.putLong(0, tgt);
        buf.clear();
        buf.limit(8);
        buf.get(buf2);
        return findGE(buf2, a);
    }

    static FindResult findGE(byte[] tgt, byte[] a) {
        assertEquals(0, a.length % N);
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

    static void show(byte[] buf, int a) {
        for (int i = 0; i < 8; i+=1) {
            System.err.print(String.format("%02x", buf[a+i]));
        }
        System.err.println();
    }

    static Mono<byte[]> openIndex(Path indexPath) {
        return ByteBufFlux.fromPath(indexPath)
        .aggregate()
        .map(buf -> {
            long nLong = buf.readableBytes();
            assertTrue(nLong < Integer.MAX_VALUE);
            int n = (int)nLong;
            assertEquals(0, (n-2)%N);
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
