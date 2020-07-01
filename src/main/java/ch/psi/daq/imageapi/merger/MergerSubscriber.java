package ch.psi.daq.imageapi.merger;

import ch.psi.daq.imageapi.eventmap.ts.EventBlobToV1MapTs;
import ch.psi.daq.imageapi.eventmap.ts.Item;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class MergerSubscriber implements Subscriber<Item> {
    Logger LOGGER = (Logger) LoggerFactory.getLogger("MergerSubscriber");
    final Merger merger;
    Subscription sub;
    Item item;
    int id;
    boolean subError;
    boolean subComplete;
    long seenBytesFromUpstream;
    boolean itemTerm;
    State state = State.Fresh;
    AtomicLong nreq = new AtomicLong();

    public enum State {
        Fresh,
        Subscribed,
        Terminated,
        Released,
    }

    MergerSubscriber(Merger merger, int id) {
        Level level = LOGGER.getEffectiveLevel();
        LOGGER = (Logger) LoggerFactory.getLogger(String.format("MergerSubscriber_%02d", id));
        LOGGER.setLevel(level);
        this.merger = merger;
        this.id = id;
        this.nreq.set(0);
    }

    @Override
    public void onSubscribe(Subscription sub) {
        synchronized (merger) {
            LOGGER.info("{}  onSubscribe", id);
            if (sub == null) {
                LOGGER.error(String.format("Publisher for id %d passed null Subscription", id));
                merger.selfError(new RuntimeException(String.format("Publisher for id %d passed null Subscription", id)));
                return;
            }
            if (state != State.Fresh) {
                LOGGER.error("onSubscribe but state != State.Fresh");
                merger.selfError(new RuntimeException("onSubscribe but state != State.Fresh"));
                return;
            }
            this.sub = sub;
            state = State.Subscribed;
            LOGGER.info("{}  onSubscribe RETURN", id);
        }
    }

    @Override
    public void onNext(Item it) {
        synchronized (merger) {
            LOGGER.debug("{}  onNext  nreq {}  item {}", id, nreq, it);
            try {
                if (state != State.Subscribed) {
                    LOGGER.warn(String.format("onNext  id %d  but not Subscribed, ignoring.", id));
                    return;
                }
                if (nreq.get() <= 0) {
                    LOGGER.info("{}  next unexpected nreq {}", id, nreq);
                    merger.selfError(new RuntimeException("logic"));
                    return;
                }
                nreq.addAndGet(-1);
                long bs = 0;
                if (it.item1 != null) {
                    if (it.item1.buf != null) {
                        bs += it.item1.buf.readableByteCount();
                    }
                }
                if (it.item2 != null) {
                    if (it.item2.buf != null) {
                        bs += it.item2.buf.readableByteCount();
                    }
                }
                if (it.item1 != null && it.item1.buf != null) {
                    long n = it.item1.buf.readableByteCount();
                    seenBytesFromUpstream += n;
                    merger.totalSeenBytesFromUpstream += n;
                }
                if (it.item2 != null && it.item2.buf != null) {
                    long n = it.item2.buf.readableByteCount();
                    seenBytesFromUpstream += n;
                    merger.totalSeenBytesFromUpstream += n;
                }
                if (item != null) {
                    item.release();
                    item = null;
                }
                if (it.isTerm()) {
                    itemTerm = true;
                }
                else if (!it.isPlainBuffer() && !it.hasMoreMarkers()) {
                    // nothing to do
                }
                else {
                    if (!it.verify()) {
                        merger.selfError(new RuntimeException("bad item"));
                        return;
                    }
                    item = it;
                    it = null;
                }
                merger.next(id);
            }
            finally {
                if (it != null) {
                    it.release();
                }
            }
            LOGGER.debug("{}  onNext RETURN", id);
        }
    }

    @Override
    public void onError(Throwable e) {
        synchronized (merger) {
            if (state != State.Subscribed && state != State.Terminated) {
                LOGGER.error("onError received in invalid state");
                return;
            }
            if (state != State.Subscribed) {
                return;
            }
            LOGGER.error("{}  onError: {}", id, e.toString());
            subError = true;
            state = State.Terminated;
            nreq.set(0);
            merger.selfError(e);
            LOGGER.error("{}  onError RETURN", id);
        }
    }

    @Override
    public void onComplete() {
        synchronized (merger) {
            if (state != State.Subscribed && state != State.Terminated) {
                LOGGER.error("onComplete received in invalid state");
                return;
            }
            if (state != State.Subscribed) {
                return;
            }
            LOGGER.info("{}  onComplete  nreq {}  seenBytesFromUpstream {}  merger.totalSeenBytesFromUpstream {}", id, nreq, seenBytesFromUpstream, merger.totalSeenBytesFromUpstream);
            subComplete = true;
            state = State.Terminated;
            nreq.set(0);
            merger.signal(id);
            LOGGER.info("{}  onComplete RETURN", id);
        }
    }

    public boolean maybeMoreItems() {
        synchronized (merger) {
            return state == State.Subscribed && !itemTerm;
        }
    }

    public void request() {
        synchronized (merger) {
            if (state != State.Subscribed) {
                LOGGER.info("request called without Subscribed");
                return;
            }
            LOGGER.debug("{}  request", id);
            nreq.addAndGet(1);
            sub.request(1);
            LOGGER.debug("{}  request RETURN", id);
        }
    }

    public long nreq() {
        synchronized (merger) {
            return nreq.get();
        }
    }

    public void cancel() {
        synchronized (merger) {
            LOGGER.info("cancel");
            state = State.Terminated;
            sub.cancel();
            LOGGER.info("cancel RETURN");
        }
    }

    public String stateInfo() {
        String desc = "";
        if (item != null) {
            if (item.isPlainBuffer()) {
                desc = "isPlainBuffer";
            }
            else if (item.hasMoreMarkers()) {
                desc = "hasMoreMarkers";
            }
            else {
                desc = "emptyNonNull";
            }
        }
        return String.format("state %s  has item %s  %s   %s", state, item != null, desc, item);
    }

    public boolean hasItem() {
        synchronized (merger) {
            return item != null;
        }
    }

    public boolean hasMoreMarkers() {
        synchronized (merger) {
            return item != null && item.hasMoreMarkers();
        }
    }

    public Item getItem() {
        synchronized (merger) {
            return item;
        }
    }

    public void itemAdvOrRemove() {
        synchronized (merger) {
            if (item != null) {
                item.adv();
                if (item.item1 == null) {
                    item = null;
                }
            }
        }
    }

    public void releaseItem() {
        synchronized (merger) {
            if (item != null) {
                item.release();
                item = null;
            }
        }
    }

    public void release() {
        if (state == State.Released) {
            LOGGER.warn("already Released");
        }
        if (sub != null) {
            sub.cancel();
            sub = null;
        }
        if (item != null) {
            item.release();
            item = null;
        }
        state = State.Released;
    }

}
