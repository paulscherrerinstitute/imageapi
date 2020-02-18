package ch.psi.daq.imageapi3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class FileManagerProd implements IFileManager {
    static Logger LOGGER = LoggerFactory.getLogger(FileManagerProd.class);
    final FileManager fileManager;

    FileManagerProd(FileManager fileManager) {
        this.fileManager = fileManager;
    }

    public static FileManagerProd fromFileManager(FileManager fileManager) {
        FileManagerProd ret = new FileManagerProd(fileManager);
        return ret;
    }

    @Override
    public Collection<String> getChannelNames() {
        return fileManager.getChannelNames();
    }

    @Override
    public List<List<Path>> locateDataFiles(String channelName, Instant begin, Instant end) {
        try {
            return fileManager.locateDataFiles(channelName, begin, end);
        }
        catch (IOException e) {
            LOGGER.error("IOException in locateDataFiles {}", e.toString());
            return Collections.emptyList();
        }
    }

    @Override
    public Path locateConfigFile(String channelName) { return fileManager.locateConfigFile(channelName); }

}
