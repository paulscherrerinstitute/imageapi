package ch.psi.daq.imageapi.merger;

import ch.psi.daq.imageapi.eventmap.ts.Item;
import ch.psi.daq.imageapi.eventmap.ts.ItemP;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.core.io.buffer.*;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Merger implements Publisher<DataBuffer> {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger("Merger");
    static Marker markerNesting = MarkerFactory.getMarker("MergerNesting");
    String channelName;
    boolean doTrace = LOGGER.isTraceEnabled();
    List<Flux<Item>> inp;
    int nsubscribed;
    long nreqd;
    Subscriber<? super DataBuffer> scrd;
    List<MergerSubscriber> scrs = new ArrayList<>();
    int state;
    int inpix;
    long lastTs;
    DataBuffer cbuf;
    DataBufferFactory bufFac;
    DataBufferFactory bufFac2;
    int bufferSize;
    int inAssemble;
    int redoAssemble;
    boolean cancelled;
    boolean finalCompleteDone;
    boolean released;
    long totalSeenBytesFromUpstream;
    long writtenBytes;
    boolean requireBufferType = false;
    int chunkEmit = Integer.MIN_VALUE;
    int chunkExpect = Integer.MIN_VALUE;
    int assembleInnerReturn = -1;
    int assembleInnerBreakReason = -1;

    enum WriteType {
        FULL,
        BEGIN,
        BLOB,
        END,
    }

    public class Written {
        int uix;
        int pos;
        int end;
        long ts;
        WriteType wty;
        public Written(int uix, int pos, int end, long ts, WriteType wty) {
            this.uix = uix;
            this.pos = pos;
            this.end = end;
            this.ts = ts;
            this.wty = wty;
        }
        public String toString() {
            return String.format("Written { uix %2d  pos %10d  end %10d  ts %16d  wty %5s }", uix, pos, end, ts, wty);
        }
    }

    List<Written> writtenLog = new ArrayList<>();

    public Merger(String channelName, List<Flux<Item>> inp, DataBufferFactory bufFac, int bufferSize) {
        this.channelName = channelName;
        this.inp = inp;
        this.bufFac2 = bufFac;
        this.bufFac = new DefaultDataBufferFactory();
        this.bufferSize = 3 * bufferSize;
        this.cbuf = bufFac.allocateBuffer(bufferSize);
        if (!checkBuf(this.cbuf)) {
            throw new RuntimeException("checkBuf");
        }
        for (int i3 = 0; i3 < inp.size(); i3 += 1) {
            MergerSubscriber scr = new MergerSubscriber(this, channelName, i3);
            scrs.add(scr);
        }
        for (int i3 = 0; i3 < inp.size(); i3 += 1) {
            Flux<Item> fl = inp.get(i3);
            fl.subscribe(scrs.get(i3));
        }
    }

    boolean checkBuf(DataBuffer buf) {
        if (buf == null) {
            LOGGER.error("checkBuf  buf == null");
            selfError(new RuntimeException("checkBuf"));
            return false;
        }
        if (buf instanceof NettyDataBuffer) {
            ByteBuf b1 = ((NettyDataBuffer) buf).getNativeBuffer();
            if (b1.refCnt() < 1) {
                LOGGER.error("checkBuf  unexpected count {}", b1.refCnt());
                selfError(new RuntimeException("checkBuf"));
                return false;
            }
        }
        else if (requireBufferType) {
            LOGGER.error("checkBuf  unexpected buffer type");
            selfError(new RuntimeException("checkBuf"));
            return false;
        }
        return true;
    }

    boolean isWritableBuffer(DataBuffer buf) {
        if (buf instanceof NettyDataBuffer) {
            NettyDataBuffer b1 = (NettyDataBuffer) buf;
            try {
                b1.getNativeBuffer().ensureWritable(1);
                return true;
            }
            catch (Throwable e) {
                LOGGER.error("\n\nisWritableBuffer\n\n");
                return false;
            }
        }
        else {
            return true;
        }
    }

    @Override
    public synchronized void subscribe(Subscriber<? super DataBuffer> scr) {
        if (nsubscribed > 0) {
            throw new RuntimeException("logic");
        }
        nsubscribed += 1;
        this.scrd = scr;
        this.scrd.onSubscribe(new MergerSubscription(this));
    }

    public synchronized void request(long n, int inReq) {
        LOGGER.trace(markerNesting, "Merger::request  BEGIN  n {}  inReq {}  nreqd {}", n, inReq, nreqd);
        if (n == Long.MAX_VALUE) {
            if (nreqd != 0) {
                LOGGER.warn("Request unbounded even though previous request!  nreqd: {}", nreqd);
            }
            nreqd = Long.MAX_VALUE;
        }
        else if (n < 1) {
            LOGGER.error("logic");
            selfError(new RuntimeException("logic"));
        }
        else if (n > Long.MAX_VALUE / 3 && nreqd > Long.MAX_VALUE / 3) {
            LOGGER.error("large but bounded request");
            selfError(new RuntimeException("logic"));
        }
        else {
            nreqd += n;
        }
        LOGGER.trace(markerNesting, "Merger::request  DO     n {}  inReq {}  nreqd {}", n, inReq, nreqd);
        if (inReq < 0) {
            LOGGER.error("logic");
            selfError(new RuntimeException("logic"));
        }
        else if (inReq == 0) {
            assemble(-1);
        }
        else if (inAssemble <= 0) {
            LOGGER.error("logic");
            selfError(new RuntimeException("logic"));
        }
        LOGGER.trace(markerNesting, "Merger::request  END    n {}  inReq {}  nreqd {}", n, inReq, nreqd);
    }

    public synchronized void cancel() {
        if (cbuf == null) {
            LOGGER.error("cbuf already null on cancel");
        }
        cancelled = true;
        for (MergerSubscriber sub : scrs) {
            sub.cancel();
        }
        release();
    }

    synchronized void release() {
        LOGGER.debug("release BEGIN");
        for (MergerSubscriber scr : scrs) {
            scr.release();
        }
        if (cbuf != null) {
            DataBufferUtils.release(cbuf);
            cbuf = null;
        }
        released = true;
        LOGGER.debug("release END");
    }

    public synchronized void next(int id) {
        long nrequ = nrequ();
        LOGGER.trace(markerNesting, "Merger::next  BEGIN  id {}  nrequ {}", id, nrequ);
        if (cancelled) {
            LOGGER.warn("next called even though cancelled  id: {}", id);
            return;
        }
        if (released) {
            LOGGER.warn("next called even though released  id: {}", id);
            return;
        }
        try {
            LOGGER.trace("{}  next  nrequ: {}", id, nrequ);
            if (nrequ == 0) {
                assemble(id);
            }
            else {
                LOGGER.trace("{}  request waiting for outstanding upstream", id);
            }
        }
        catch (Throwable e) {
            LOGGER.error("{}  err in next: {}", id, e.toString());
            selfError(e);
        }
        LOGGER.trace(markerNesting, "Merger::next  END    id {}  nrequ {}", id, nrequ);
    }

    public synchronized void signal(int id) {
        long nrequ = nrequ();
        LOGGER.trace(markerNesting, "Merger::signal  BEGIN  id {}  nrequ {}", id, nrequ);
        if (cancelled) {
            LOGGER.warn("signal called even though cancelled  id: {}", id);
            return;
        }
        if (released) {
            LOGGER.warn("signal called even though released  id: {}", id);
            return;
        }
        try {
            if (nrequ == 0) {
                assemble(id);
            }
            else {
                LOGGER.warn("{}  signal waiting for outstanding upstream", id);
            }
        }
        catch (Throwable e) {
            LOGGER.error("{}  err in signal: {}", e.toString(), id);
            selfError(e);
        }
        LOGGER.trace(markerNesting, "Merger::signal  END    id {}  nrequ {}", id, nrequ);
    }

    synchronized void assemble(int id) {
        LOGGER.trace(markerNesting, "Merger::assemble  BEGIN  id {}  redoAssemble {}", id, redoAssemble);
        if (cancelled) {
            LOGGER.warn("in assemble():  already cancelled, no need to assemble");
            return;
        }
        if (cbuf == null) {
            if (finalCompleteDone) {
                LOGGER.warn("cbuf null, but already complete anyway");
                return;
            }
            else {
                LOGGER.error("in assemble but cbuf null");
                throw new RuntimeException("logic");
            }
        }
        if (inAssemble > 0) {
            LOGGER.trace(markerNesting, "Merger::assemble  SKIP   id {}  redoAssemble {}", id, redoAssemble);
            redoAssemble += 1;
        }
        else {
            try {
                inAssemble += 1;
                assembleInner(id);
                while (redoAssemble != 0) {
                    if (redoAssemble < 0) {
                        LOGGER.error("redoAssemble < 0");
                        selfError(new RuntimeException("redoAssemble < 0"));
                        return;
                    }
                    LOGGER.trace(markerNesting, "Merger::assemble  REDO   id {}  redoAssemble {}", id, redoAssemble);
                    redoAssemble -= 1;
                    assembleInner(id);
                }
            }
            finally {
                inAssemble -= 1;
            }
            LOGGER.trace(markerNesting, "Merger::assemble  END    id {}  redoAssemble {}", id, redoAssemble);
        }
    }

    void assembleInner(int id) {
        LOGGER.trace(markerNesting, "Merger::assembleInner  BEGIN  id {}  nreqd {}", id, nreqd);
        assembleInnerReturn = -1;
        assembleInnerBreakReason = -1;
        if (nrequ() != 0) {
            LOGGER.debug("assembleInner request waiting for outstanding upstream");
            return;
        }
        if (cbuf == null) {
            LOGGER.error("~~~~~~~~~~~~~~~  cbuf already null, maybe complete?");
            return;
        }
        while (!cancelled && nreqd > 0) {
            if (writtenLog.size() > 32) {
                int i2 = writtenLog.size() - 4;
                for (int i1 = 0; i1 < 4; i1 += 1) {
                    writtenLog.set(i1, writtenLog.get(i2));
                    i2 += 1;
                }
                while (writtenLog.size() > 4) {
                    writtenLog.remove(writtenLog.size() - 1);
                }
            }
            LOGGER.trace("assemble loop  state: {}  inpix: {}", state, inpix);
            for (int i1 = 0; i1 < scrs.size(); i1 += 1) {
                MergerSubscriber scr = scrs.get(i1);
                LOGGER.trace("scr  i1 {}  term {}  item {}", i1, scr.itemTerm, scr.item);
                while (scr.item != null && !scr.item.isPlainBuffer() && !scr.item.hasMoreMarkers()) {
                    scr.itemAdvOrRemove();
                    LOGGER.debug("useless item, advanced, now item {} {}", i1, scr.item);
                }
            }
            if (nreqd <= 0) {
                LOGGER.warn("Potential 2 upcoming write with nreqd: {}", nreqd);
            }
            if (state == 0) {
                long tsm = Long.MAX_VALUE;
                int i2 = -1;
                int validTsCompared = 0;
                int nNoMoreItems = 0;
                int nItemNull = 0;
                int nItem1Null = 0;
                int nItemNullMaybeMoreItems = 0;
                int nNeedsMoreMaybeMoreItems = 0;
                int nNoPositionsMarkedMaybeMoreItems = 0;
                int nNoPositionsMarked = 0;
                for (int i1 = 0; i1 < scrs.size(); i1 += 1) {
                    MergerSubscriber scr = scrs.get(i1);
                    if (!scr.maybeMoreItems()) {
                        nNoMoreItems += 1;
                    }
                    if (scr.item == null) {
                        if (scr.maybeMoreItems()) {
                            nItemNullMaybeMoreItems += 1;
                        }
                        else {
                            nItemNull += 1;
                        }
                    }
                    else {
                        if (!scr.hasMoreMarkers() && scr.maybeMoreItems()) {
                            nNeedsMoreMaybeMoreItems += 1;
                        }
                        else if (scr.item.item1 == null) {
                            nItem1Null += 1;
                        }
                        else if (scr.item.item1.c < 0) {
                            selfError(new RuntimeException("weird c"));
                        }
                        else if (scr.item.item1.c == 0) {
                            if (scr.maybeMoreItems()) {
                                nNoPositionsMarkedMaybeMoreItems += 1;
                                scr.releaseItem();
                            }
                            else {
                                nNoPositionsMarked += 1;
                            }
                        }
                        else if (scr.item.ix < 0 || scr.item.ix >= scr.item.item1.c) {
                            selfError(new RuntimeException("weird ix"));
                            return;
                        }
                        else {
                            if (scr.item.item1.ty[scr.item.ix] != 1) {
                                selfError(new RuntimeException("logic"));
                                return;
                            }
                            long ts = scr.item.item1.ts[scr.item.ix];
                            if (ts < tsm) {
                                tsm = ts;
                                i2 = i1;
                            }
                            validTsCompared += 1;
                        }
                    }
                }
                if (tsm == Long.MAX_VALUE) {
                    tsm = 0;
                }
                if (doTrace) {
                    LOGGER.trace("tsm {} {}  .. validTsCompared {}  nItemNull {}  nItemNullMaybeMoreItems {}  nNeedsMoreMaybeMoreItems {}  nItem1Null {}  nNoMoreItems {}  nNoPositionsMarkedMaybeMoreItems {}  nNoPositionsMarked {}",
                    tsm / 1000000000L, tsm % 1000000000L, validTsCompared, nItemNull, nItemNullMaybeMoreItems, nNeedsMoreMaybeMoreItems, nItem1Null, nNoMoreItems, nNoPositionsMarkedMaybeMoreItems, nNoPositionsMarked);
                }
                if (nItemNullMaybeMoreItems > 0) {
                    i2 = -1;
                }
                if (nNeedsMoreMaybeMoreItems > 0) {
                    i2 = -1;
                }
                if (nNoPositionsMarkedMaybeMoreItems > 0) {
                    i2 = -1;
                }
                if (i2 < 0) {
                    LOGGER.trace(markerNesting, "-------   choice not possible   ----------");
                    assembleInnerBreakReason = 1;
                    break;
                }
                inpix = i2;
                MergerSubscriber fscr = scrs.get(i2);
                int six = fscr.item.ix;
                if (fscr.item.item1.buf == null) {
                    selfError(new RuntimeException("logic"));
                    return;
                }
                if (nreqd <= 0) {
                    LOGGER.warn("Potential upcoming write with nreqd: {}", nreqd);
                }
                if (six + 1 < fscr.item.item1.c) {
                    if (fscr.item.item1.ty[six + 1] != 2) {
                        selfError(new RuntimeException("logic"));
                        return;
                    }
                    if (fscr.item.item1.ts[six + 1] != tsm) {
                        selfError(new RuntimeException("logic"));
                        return;
                    }
                    DataBuffer buf = fscr.item.item1.buf;
                    int pos1 = fscr.item.item1.pos[six];
                    int pos2 = fscr.item.item1.pos[six + 1];
                    int pos = pos1;
                    int n = pos2 - pos1;
                    long ts = fscr.item.item1.ts[six];
                    ByteBuffer bb = buf.asByteBuffer(0, buf.capacity());
                    int len1 = bb.getInt(pos);
                    LOGGER.trace("full chunk in same buffer  inpix {}  six {}  pos {}  ts {}  n {}  len1 {}", inpix, six, pos, ts, n, len1);
                    int len2 = bb.getInt(pos + len1 - 4);
                    if (len1 != len2) {
                        LOGGER.error("len mismatch  {} vs {}", len1, len2);
                        dumpState();
                        selfError(new RuntimeException("bad"));
                        return;
                    }
                    chunkExpect = len1;
                    if (chunkExpect != n) {
                        LOGGER.error("full chunk in same buffer copy  six {}  pos {}  n {}  chunkExpect {}", six, pos, n, chunkExpect);
                        dumpState();
                        selfError(new RuntimeException("bad"));
                        return;
                    }
                    if (!writeOutput(buf, pos, n)) {
                        assembleInnerReturn = 1;
                        return;
                    }
                    writtenLog.add(new Written(inpix, pos, pos + n, tsm, WriteType.FULL));
                    chunkExpect = Integer.MIN_VALUE;
                    chunkEmit = Integer.MIN_VALUE;
                    fscr.itemAdvOrRemove();
                    fscr.itemAdvOrRemove();
                }
                else {
                    DataBuffer buf = fscr.item.item1.buf;
                    int pos = fscr.item.item1.pos[six];
                    long ts = fscr.item.item1.ts[six];
                    int n = fscr.item.item1.p2 - pos;
                    ByteBuffer bb = buf.asByteBuffer(0, buf.capacity());
                    int len1 = bb.getInt(pos);
                    LOGGER.trace("begin chunk  inpix {}  six {}  pos {}  ts {}  n {}  len1 {}", inpix, six, pos, ts, n, len1);
                    chunkExpect = len1;
                    if (nreqd <= 0) {
                        selfError(new RuntimeException(String.format("about to call writeOutput with nreqd: %d", nreqd)));
                        return;
                    }
                    chunkEmit = n;
                    if (!writeOutput(buf, pos, n)) {
                        assembleInnerReturn = 2;
                        return;
                    }
                    writtenLog.add(new Written(inpix, pos, pos + n, tsm, WriteType.BEGIN));
                    fscr.itemAdvOrRemove();
                    LOGGER.trace("after begin chunk and advance  {}", fscr.item);
                    state = 1;
                    lastTs = tsm;
                }
            }
            else if (state == 1) {
                MergerSubscriber fscr = scrs.get(inpix);
                LOGGER.trace("state {}  inpix {}  item {}", state, inpix, fscr != null ? fscr.item : null);
                if (fscr == null) {
                    LOGGER.error("MergerSubscriber vanished");
                    selfError(new RuntimeException("MergerSubscriber vanished"));
                    return;
                }
                if (!fscr.hasItem()) {
                    LOGGER.trace(markerNesting, "state {}  inpix {}  NO ITEM  breaking", state, inpix);
                    assembleInnerBreakReason = 2;
                    break;
                }
                Item item = fscr.getItem();
                if (item.isPlainBuffer()) {
                    ItemP item1 = item.item1;
                    DataBuffer buf = item1.buf;
                    int pos = item1.p1;
                    int n = item1.p2 - item1.p1;
                    LOGGER.trace("plain buffer inpix {}  pos {}  n {}  buf {}", inpix, pos, n, buf);
                    if (nreqd <= 0) {
                        selfError(new RuntimeException(String.format("about to call writeOutput with nreqd: %d", nreqd)));
                        return;
                    }
                    chunkEmit += n;
                    if (!writeOutput(buf, pos, n)) {
                        assembleInnerReturn = 3;
                        return;
                    }
                    writtenLog.add(new Written(inpix, pos, pos + n, lastTs, WriteType.BLOB));
                    fscr.itemAdvOrRemove();
                }
                else if (fscr.hasMoreMarkers()) {
                    int six = fscr.item.ix;
                    if (fscr.item.item1.ty[six] != 2) {
                        selfError(new RuntimeException("logic"));
                        return;
                    }
                    if (fscr.item.item1.ts[six] != lastTs) {
                        selfError(new RuntimeException("logic"));
                        return;
                    }
                    DataBuffer buf = fscr.item.item1.buf;
                    int pos = fscr.item.item1.p1;
                    int n = fscr.item.item1.pos[six] - pos;
                    long ts = fscr.item.item1.ts[six];
                    int len2 = buf.asByteBuffer(0, buf.capacity()).getInt(pos + n - 4);
                    if (len2 != chunkExpect) {
                        LOGGER.error("terminal len mismatch  chunkExpect {}  len2 {}", chunkExpect, len2);
                        dumpState();
                        selfError(new RuntimeException("bad"));
                        return;
                    }
                    if (nreqd <= 0) {
                        selfError(new RuntimeException(String.format("about to call writeOutput with nreqd: %d", nreqd)));
                        return;
                    }
                    chunkEmit += n;
                    if (chunkEmit != chunkExpect) {
                        LOGGER.error("chunkExpect != chunkEmit  chunkExpect {}  chunkEmit {}", chunkExpect, chunkEmit);
                        dumpState();
                        selfError(new RuntimeException("bad"));
                        assembleInnerReturn = 13;
                        return;
                    }
                    if (!writeOutput(buf, pos, n)) {
                        assembleInnerReturn = 4;
                        return;
                    }
                    writtenLog.add(new Written(inpix, pos, pos + n, ts, WriteType.END));
                    LOGGER.trace(markerNesting, "terminated buffer  inpix {}  pos {}  n {}  ts {}  buf {}", inpix, pos, n, ts, buf);
                    fscr.itemAdvOrRemove();
                    chunkExpect = Integer.MIN_VALUE;
                    chunkEmit = Integer.MIN_VALUE;
                    state = 0;
                    inpix = -1;
                    lastTs = -1;
                }
                else {
                    LOGGER.error("state {}  no plain, no has more", state);
                    selfError(new RuntimeException(String.format("state %d  no plain, no has more", state)));
                    return;
                }
            }
            else {
                LOGGER.error("logic");
                scrd.onError(new RuntimeException("logic"));
                assembleInnerReturn = 13;
                return;
            }
        }
        if (cancelled) {
            LOGGER.debug("assembleInner  do not check for refill because cancelled");
            assembleInnerReturn = 2;
            return;
        }
        if (nreqd > 0) {
            int nReqUp = 0;
            for (int i1 = 0; i1 < scrs.size(); i1 += 1) {
                MergerSubscriber scr = scrs.get(i1);
                if (!scr.hasItem() && scr.maybeMoreItems()) {
                    LOGGER.debug("Request next item for {}  item null: {}  isPlainBuffer: {}  isTerm: {}",
                    i1, scr.item == null, scr.item != null && scr.item.isPlainBuffer(), scr.item != null && scr.item.isTerm());
                    scr.request();
                    nReqUp += 1;
                }
            }
            if (nReqUp == 0) {
                LOGGER.trace(markerNesting, "Merger  refill  nReqUp == 0");
                boolean allCompleteAndEmpty = scrs.stream().allMatch(scr -> !scr.hasItem() && !scr.maybeMoreItems());
                if (allCompleteAndEmpty) {
                    LOGGER.trace(markerNesting, "Merger  refill  nReqUp == 0 && allCompleteAndEmpty");
                    finalComplete();
                    if (redoAssemble != 0) {
                        LOGGER.trace(markerNesting, "Merger  refill  nReqUp == 0 && allCompleteAndEmpty  RESET REDO COUNT");
                        redoAssemble = 0;
                    }
                }
                else {
                    LOGGER.trace(markerNesting, "Merger  refill  nReqUp == 0 && !allCompleteAndEmpty");
                    dumpState();
                }
            }
        }
        assembleInnerReturn = 0;
        LOGGER.trace(markerNesting, "Merger::assembleInner  END    id {}  nreqd {}", id, nreqd);
    }

    long writeOutputN = 0;

    synchronized boolean writeOutput(DataBuffer src, int pos, int n) {
        writeOutputN += 1;
        if (!checkBuf(cbuf)) {
            LOGGER.error("checkBuf error");
            selfError(new RuntimeException("checkBuf error"));
            return false;
        }
        if (cancelled) {
            LOGGER.error("writeOutput despite cancelled");
            return false;
        }
        if (src == null) {
            LOGGER.error("writeOutput with src null");
            selfError(new RuntimeException("writeOutput with src null"));
            return false;
        }
        if (nreqd <= 0) {
            LOGGER.error("writeOutput called even without nreqd");
            selfError(new RuntimeException("writeOutput called even without nreqd"));
            return false;
        }
        if (n > cbuf.writableByteCount()) {
            LOGGER.trace("writeOutput  not enough space  {}  {}", n, cbuf.writableByteCount());
            DataBuffer xbuf = cbuf;
            cbuf = bufFac.allocateBuffer(bufferSize);
            LOGGER.trace(markerNesting, "Merger  onNext  item to downstream  BEGIN  writeOutputN {}", writeOutputN);
            nreqd -= 1;
            scrd.onNext(xbuf);
            LOGGER.trace(markerNesting, "Merger  onNext  item to downstream  END    writeOutputN {}", writeOutputN);
            if (!isWritableBuffer(cbuf)) {
                LOGGER.error("buffer not writable");
                selfError(new RuntimeException("buffer not writable"));
                return false;
            }
        }
        if (cancelled) {
            LOGGER.warn("writeOutput  downstream has cancelled, return early");
            return false;
        }
        if (n > cbuf.writableByteCount()) {
            if (cbuf.capacity() > 50 * 1024 * 1024) {
                LOGGER.error("cbuf still too small");
                selfError(new RuntimeException("cbuf still too small"));
                return false;
            }
            else {
                int bs0 = bufferSize;
                while (bufferSize <= 50 * 1024 * 1024 && bufferSize < n + bs0) {
                    bufferSize *= 2;
                }
                cbuf.ensureCapacity(bufferSize);
                if (n > cbuf.writableByteCount()) {
                    LOGGER.error("can not allocate enough space to write  n: {}", n);
                    selfError(new RuntimeException("can not allocate space to write"));
                    return false;
                }
            }
        }
        DataBuffer sl = src.slice(pos, n);
        cbuf.write(sl);
        writtenBytes += sl.readableByteCount();
        return true;
    }

    synchronized void finalComplete() {
        LOGGER.warn("finalComplete  writtenBytes {}", writtenBytes);
        finalCompleteDone = true;
        if (cbuf != null) {
            if (nreqd > 0) {
                DataBuffer xbuf = cbuf;
                cbuf = null;
                nreqd -= 1;
                scrd.onNext(xbuf);
            }
            else {
                LOGGER.error("DATALOSS: {}", cbuf.readableByteCount());
            }
        }
        else {
            LOGGER.info("finalComplete, no data to flush");
        }
        release();
        scrd.onComplete();
        LOGGER.warn("finalComplete RETURN");
    }

    synchronized void selfError(Throwable e) {
        StringBuilder sb = new StringBuilder();
        formatState(sb);
        LOGGER.error("selfError  {}", sb.toString());
        for (MergerSubscriber scr : scrs) {
            scr.cancel();
        }
        if (scrd != null) {
            scrd.onError(e);
        }
        else {
            LOGGER.error("can not signal error\n{}", e.toString());
        }
        LOGGER.error("selfError RETURN");
    }

    long nrequ() {
        return scrs.stream().map(MergerSubscriber::nreq).reduce(0L, Long::sum);
    }

    public synchronized void formatSubscribers(StringBuilder sb) {
        for (MergerSubscriber sub : scrs) {
            sub.formatState(sb);
            sb.append("\n\n");
        }
    }

    void formatWrittenLog(StringBuilder sb) {
        for (Written w : writtenLog) {
            sb.append(w.toString()).append("\n");
        }
    }

    public synchronized void formatState(StringBuilder sb) {
        sb.append(String.format("writtenBytes %d  nreqd %d  nrequ %d  assembleInnerReturn %d  assembleInnerBreakReason %d  redoAssemble %d  cancelled %s", writtenBytes, nreqd, nrequ(), assembleInnerReturn, assembleInnerBreakReason, redoAssemble, cancelled)).append("\n");
        formatSubscribers(sb);
        formatWrittenLog(sb);
    }

    void dumpState() {
        StringBuilder sb = new StringBuilder();
        formatState(sb);
        LOGGER.error("MERGER STATE\n{}", sb.toString());
    }

}
