package ch.psi.daq.imageapi.eventmap.value;

import java.util.List;

public class MapJsonChannelStart implements MapJsonItem {
    public String name;
    public String type;
    public String byteOrder;
    public List<Integer> shape;
    public boolean array;

    @Override
    public void release() {
    }

}
