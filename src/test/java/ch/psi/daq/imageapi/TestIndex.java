package ch.psi.daq.imageapi;

import com.google.common.io.BaseEncoding;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;

import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static ch.psi.daq.imageapi.Index.show;
import static ch.psi.daq.imageapi.Index.cmp;

public class TestIndex {

    static final int N = 16;

    /*
    test data file: 0000000000003600000_00000_Data_Index   (47f91db6b32f1ab24b0afbd592667259a660727c)
    begin: 15f0659e5ac18ace
    end:   15f068e489881ee2
     5000  15f065fb5804e173
    10000  15f066585407c932
    23000  15f0674a320e8d77
    45000  15f068e3de0c9456
    */

    @Test
    public void find5000() {
        byte[] tgt = BaseEncoding.base16().lowerCase().decode("15f065fb5804e173");
        assertEquals(5001 * 16,
        monoTestIndex()
        .doOnNext(TestIndex::testCmp)
        .map(x -> Index.findGE(tgt, x))
        .block().i
        );
    }

    @Test
    public void find10000() {
        byte[] tgt = BaseEncoding.base16().lowerCase().decode("15f066585407c932");
        assertEquals(10000 * 16,
        monoTestIndex()
        .doOnNext(TestIndex::testCmp)
        .map(x -> Index.findGE(tgt, x))
        .block().i
        );
    }

    @Test
    public void find23000() {
        byte[] tgt = BaseEncoding.base16().lowerCase().decode("15f0674a320e8d77");
        assertEquals(23000 * 16,
        monoTestIndex()
        .doOnNext(TestIndex::testCmp)
        .map(x -> Index.findGE(tgt, x))
        .block().i
        );
    }

    @Test
    public void find45000() {
        byte[] tgt = BaseEncoding.base16().lowerCase().decode("15f068e3de0c9456");
        assertEquals(45000 * 16,
        monoTestIndex()
        .doOnNext(TestIndex::testCmp)
        .map(x -> Index.findGE(tgt, x))
        .block().i
        );
    }

    @Test
    public void findBegin() {
        byte[] tgt = BaseEncoding.base16().lowerCase().decode("15f0659e5ac18ace");
        assertEquals(0 * 16,
        monoTestIndex()
        .doOnNext(TestIndex::testCmp)
        .map(x -> Index.findGE(tgt, x))
        .block().i
        );
    }

    @Test
    public void findEnd() {
        byte[] tgt = BaseEncoding.base16().lowerCase().decode("15f068e489881ee2");
        assertEquals(45036 * 16,
        monoTestIndex()
        .doOnNext(TestIndex::testCmp)
        .map(x -> Index.findGE(tgt, x))
        .block().i
        );
    }

    @Test
    public void findPastEnd() {
        byte[] tgt = BaseEncoding.base16().lowerCase().decode("15f068e489881ee3");
        assertEquals(-1,
        monoTestIndex()
        .doOnNext(TestIndex::testCmp)
        .map(x -> Index.findGE(tgt, x))
        .block().i
        );
    }

    @Test
    public void findBeforeBegin() {
        byte[] tgt = BaseEncoding.base16().lowerCase().decode("15f068e489881ee3");
        assertEquals(-1,
        monoTestIndex()
        .doOnNext(TestIndex::testCmp)
        .map(x -> Index.findGE(tgt, x))
        .block().i
        );
    }

    @Test
    public void printLoc() {
        monoTestIndex()
        .doOnNext(buf -> {
            assertEquals("15f0674a320e8d77", show(buf, 23000 * 16));
        })
        .block();
    }

    static Mono<byte[]> monoTestIndex() {
        Path path = Path.of("test/data/47f91db6b32f1ab24b0afbd592667259a660727c");
        return Index.openIndex(path);
    }

    static void testCmp(byte[] a) {
        int n = a.length / N;
        for (int k = 0; k < n-N; k+=N) {
            int v = cmp(a, k, a, k+N);
            if (v != -1) {
                show(a, k);
                show(a, k+N);
            }
            assertEquals(-1, v);
            assertEquals(+1, cmp(a, k+N, a, k));
            assertEquals(0, cmp(a, k, a, k));
        }
    }

}
