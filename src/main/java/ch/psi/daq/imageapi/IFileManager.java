package ch.psi.daq.imageapi;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
import java.util.List;

public interface IFileManager {
    Collection<String> getChannelNames();
    List<List<Path>> locateDataFiles(String channelName, Instant start, Instant end);
    Path locateConfigFile(String channelName);
}
