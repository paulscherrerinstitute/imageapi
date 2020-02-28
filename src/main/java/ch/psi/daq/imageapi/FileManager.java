package ch.psi.daq.imageapi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileManager.class);

    public static final String CONFIG_DIR = "config";
    public static final String META_DIR = "meta";
    public static final String BY_TIME_DIR = "byTime";
    public static final String DATA_FILE_SUFFIX = "_Data";
    public static final String INDEX_FILE_SUFFIX = "_Data_Index";
    public static final String[] DATA_CLASS_SUFFIX = new String[]{"_2", "_3", "_4"}; // SCALARS, WAVEFORMS, IMAGES
    static final String CONFIG_FILE_NAME = "00000_Config";

    private final Path baseDir;
    private final int binSize;

    private Collection<Path> dataDirs;

    long updatedLast;
    Collection<String> channelNamesLast;

    public FileManager(String rootDir, String baseKeyspaceName, int binSize) {
        this.baseDir = Paths.get(rootDir + File.separator + baseKeyspaceName);
        this.binSize = binSize;
        discoverAvailableDataDirectories(baseKeyspaceName);
    }

    void discoverAvailableDataDirectories(String baseKeyspaceName) {
        try {
            dataDirs = Files.list(baseDir)
            .filter(p->p.getFileName().toString().startsWith(baseKeyspaceName))
            .map(p -> p.resolve(BY_TIME_DIR))
            .collect(Collectors.toList());
        }
        catch (IOException e) {
            LOGGER.error("Unable to locate data directories", e);
            throw new RuntimeException(e);
        }
    }

    public Collection<String> getChannelNames() {
        synchronized (this) {
            long now = System.nanoTime();
            if (channelNamesLast != null && now < updatedLast + 4L * 1000 * 1000 * 1000) {
                return channelNamesLast;
            }
            else {
                channelNamesLast = dataDirs.stream()
                .flatMap(p -> {
                    try {
                        return Files.list(p);
                    } catch (IOException e) {
                        LOGGER.warn("Unable to list directory {}", p);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .map(p -> p.getFileName().toString())
                .collect(Collectors.toList());
                updatedLast = now;
                return channelNamesLast;
            }
        }
    }

//    public List<Path> locateDataAndIndexFiles(String channelName, Instant start, Instant end){
//        Path dataDir = dataDirs.stream()
//                .map(p -> p.resolve(channelName))
//                .filter(Files::exists)
//                .findFirst()  // we assume that a channel cannot be in more than one data class - however we have no reporting if it does so far
//                .get();
//
//        // TODO maybe cache this path for faster location
//        long fileIdStart = start.toEpochMilli()/this.binSize;
//        long fileIdEnd = end.toEpochMilli()/this.binSize;
//
//        List<Path> dataFiles = new ArrayList<>();
//        for(long id = fileIdStart; id <= fileIdEnd; id++){
//            Path dataBinDir = dataDir.resolve(String.format("%019d", id));
//
//            // now we have to walk this directory recursively and find all files that end with _Data
//            try (Stream<Path> walk = Files.walk(dataBinDir)) {
//
//                List<Path> result = walk.filter(Files::isRegularFile)
//                        .filter(p-> {
//                            String filename = p.getFileName().toString();
//                            return filename.endsWith(DATA_FILE_SUFFIX) || filename.endsWith(INDEX_FILE_SUFFIX);
//                        })
//                        .collect(Collectors.toList());
//                dataFiles.addAll(result);
//            } catch (IOException e) {
//                logger.warn("Unable to locate data files", e);
//            }
//        }
//        return dataFiles;
//    }

    public List<List<Path>> locateDataFiles(String channelName, Instant start, Instant end) throws IOException {

        // Locate base directory for given channel
        Path dataDir;
        try {
            dataDir = dataDirs.stream()
            .map(p -> p.resolve(channelName))
            .filter(Files::exists)
            .findFirst()  // we assume that a channel cannot be in more than one data class - however we have no reporting if it does so far
            .get();
        }
        catch (NoSuchElementException e) {
            // There is absolutely no data for the given channel! (not even in a different time range)
            throw new NoSuchElementException(String.format("No data exists for channel %s", channelName));
        }

        long fileIdStart = start.toEpochMilli()/this.binSize;
        long fileIdEnd = end.toEpochMilli()/this.binSize;

        List<List<Path>> dataFiles = new ArrayList<>();
        for(long id = fileIdStart; id <= fileIdEnd; id++){
            Path dataBinDir = dataDir.resolve(String.format("%019d", id));

            if(dataBinDir.toFile().exists()) {
                List<Path> files;
                try (Stream<Path> splitDirsStream = Files.list(dataBinDir)) {
                    files = splitDirsStream.filter(Files::isDirectory)
                    .map(path -> {
                        try (Stream<Path> stream = Files.list(path)) {
                            return stream.filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().endsWith(DATA_FILE_SUFFIX))
                            .findFirst();
                        }
                        catch (IOException e) {
                            LOGGER.error("Unable to locate data file of the split", e);
                            throw new RuntimeException("Unable to locate data file of the split");
                        }
                    })
                    .peek(x -> {
                        if (x.isEmpty()) {
                            LOGGER.error(String.format("Empty path on channelName: %s  begin: %s  end: %s", channelName, start.toString(), end.toString()));
                        }
                    })
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
                    // now we have to walk this directory recursively and find all files that end with _Data
                }
                dataFiles.add(files);
            }
            else{
                LOGGER.error("Bin {} does not exist", dataBinDir);
            }
        }
        return dataFiles;
    }

    public Path locateConfigFile(String channelName){
        Path path = baseDir.resolve(CONFIG_DIR).resolve(channelName).resolve("latest").resolve(CONFIG_FILE_NAME);
        if (!Files.exists(path)) {
            LOGGER.warn("Unable to locate config file for channel {} at {}", channelName, path);
            throw new RuntimeException("Unable to locate config file");
        }
        return path;
    }

}
