package ch.psi.daq.imageapi.pod.api1;

import java.util.List;

public class Query {

    public List<String> channels;
    public Range range;

    // The following are for testing usage only:
    public List<Integer> splits;
    public int bufferSize;
    public int decompressOnServer;
    public long limitBytes;

}
