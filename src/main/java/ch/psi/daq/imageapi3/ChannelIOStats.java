package ch.psi.daq.imageapi3;

public class ChannelIOStats {
    public Average avg;
    public Deviation dev;

    public ChannelIOStats(Average avg, Deviation dev) {
        this.avg = avg;
        this.dev = dev;
    }

    public static ChannelIOStats create(Average avg, Deviation dev) {
        return new ChannelIOStats(avg, dev);
    }

}
