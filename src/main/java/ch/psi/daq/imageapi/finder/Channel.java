package ch.psi.daq.imageapi.finder;

public class Channel implements Comparable<Channel> {
    public BaseDir base;
    public String name;
    @Override
    public int compareTo(Channel x) {
        int n = base.compareTo(x.base);
        if (n != 0) {
            return n;
        }
        else {
            return name.compareTo(x.name);
        }
    }
    @Override
    public String toString() {
        return String.format("Channel { base: %s, name: %s }", base, name);
    }
}
