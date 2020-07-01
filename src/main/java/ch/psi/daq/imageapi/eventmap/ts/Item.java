package ch.psi.daq.imageapi.eventmap.ts;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

public class Item {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger("MapTsItem");
    public ItemP item1;
    public ItemP item2;
    public boolean term;
    public int ix;
    public boolean isLast;

    @Override
    public String toString() {
        return String.format("TsItem { ix: %d  term: %s  isPlain %s  item1: %s, item2: %s }", ix, term, isPlainBuffer(), item1, item2);
    }

    public boolean isTerm() {
        return term;
    }

    public boolean verify() {
        if (isPlainBuffer() && item1.buf == null) {
            LOGGER.error("plain buffer without buf: {}", this);
            return false;
        }
        if (term) {
            if (item1 != null) {
                LOGGER.error("has item1 despite term");
                return false;
            }
            if (item2 != null) {
                LOGGER.error("has item2 despite term  {}", this);
                return false;
            }
        }
        else {
            if (item1 == null || item1.buf == null) {
                LOGGER.error("item1 == null || item1.buf == null  {}", this);
                return false;
            }
            if (item2 != null && item2.buf == null) {
                LOGGER.error("item2 == null || item2.buf == null  {}", this);
                return false;
            }
        }
        return true;
    }

    public boolean hasMoreMarkers() {
        return item1 != null && ix < item1.c;
    }

    public boolean isPlainBuffer() {
        return !term && item1 != null && item1.c == 0 && item1.p1 != item1.p2;
    }

    public void adv() {
        if (isTerm()) {
            LOGGER.warn("adv called despite isTerm");
        }
        if (item1 == null) {
            if (item2 != null) {
                LOGGER.error("\n\nPANIC  logic error 88371\n\n");
                release();
                item2 = null;
            }
            return;
        }
        ix += 1;
        if (ix >= item1.c) {
            ix = 0;
            item1.release();
            if (item2 != null) {
                item1 = item2;
                item2 = null;
            }
            else {
                item1 = null;
            }
        }
    }

    public void release() {
        if (item1 != null) {
            item1.release();
            item1 = null;
        }
        if (item2 != null) {
            item2.release();
            item2 = null;
        }
    }

}
