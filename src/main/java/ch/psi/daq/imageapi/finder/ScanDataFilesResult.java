package ch.psi.daq.imageapi.finder;

import ch.psi.daq.imageapi.Microseconds;

import java.util.List;

public class ScanDataFilesResult {
    public String channelName;
    public List<KeyspaceOrder2> keyspaces;
    public Microseconds duration;
}
