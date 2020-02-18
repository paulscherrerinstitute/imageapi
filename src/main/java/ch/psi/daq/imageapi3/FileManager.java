package ch.psi.daq.imageapi3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileManager.class);

    public static final String CONFIG_DIR = "config";
    public static final String META_DIR = "meta";
    public static final String BY_TIME_DIR = "byTime";
    public static final String BY_PULSE_DIR = "byPulse";

    public static final String DATA_FILE_SUFFIX = "_Data";
    public static final String INDEX_FILE_SUFFIX = "_Data_Index"; // only for data classes 3 and 4 (WAVEFORMS, IMAGES)
    public static final String STATS_FILE_SUFFIX = "_Stats"; // only for data classes 3 and 4 (WAVEFORMS, IMAGES)

    public static final String[] DATA_CLASS_SUFFIX = new String[]{"_2", "_3", "_4"}; // SCALARS, WAVEFORMS, IMAGES


    private final String rootDir;
    private final String baseKeyspaceName;
    private final Path baseDir;
    private final int binSize; // up to now bin size for images was 3600000, all others 86400000 (defined in dispatcher.properties)

    private Collection<Path> dataDirs;

    long updatedLast;
    Collection<String> channelNamesLast;

    /**
     *
     * Directory structure for data
     * &lt;root_dir&gt; # usually /data/sf-databuffer
     *     &lt;base_keyspace_name&gt; # daq_swissfel = baseDir
     *         config
     *             &lt;channel_name&gt;
     *                 latest
     *                     00000_Config # 00000 = version number serializer
     *             ...
     *         &lt;base_keyspace_name&gt;_2 # Scalar
     *             byTime
     *                 &lt;channel_name&gt;
     *                     0000000000000017741 # bin = time(ms)/bin_size(ms) / increasing number
     *                         0000000000 # split starting at 0 - basically this is the machine/writer id/number
     *                             0000000000086400000_00000_Data  # 0000000000086400000 = bin_size(ms)  00000 = Serializer version
     *                             0000000000086400000_00000_Data_Index  # Index file for data - used for waveforms/images only
     *                             0000000000086400000_00000_Stats  # ????
     *                             0000000000086400000_Data_TTL # ????
     *                             0000000000086400000_Stats_TTL # ????
     *         &lt;base_keyspace_name&gt;_3 # Waveforms
     *         &lt;base_keyspace_name&gt;_4 # Images
     *         meta
     *             byPulse
     *             byTime
     *
     * @param rootDir
     * @param baseKeyspaceName
     */
    public FileManager(@Value("${rootDir}") String rootDir, @Value("${baseKeyspaceName}") String baseKeyspaceName, @Value("${binSize}") int binSize){
        this.rootDir = rootDir;
        this.baseKeyspaceName = baseKeyspaceName;
        this.baseDir = Paths.get(rootDir + File.separator + baseKeyspaceName);
        this.binSize = binSize;

        // Find out all available data directories
        try {
            dataDirs = Files.list(getBaseDir())
            .filter(p->p.getFileName().toString().startsWith(baseKeyspaceName))
            .map(p -> p.resolve(BY_TIME_DIR))  // So far everything was stored by time so we append this
            .collect(Collectors.toList());
        } catch (IOException e) {
            LOGGER.error("Unable to locate data directories", e);
            throw new RuntimeException(e);
        }
    }

    public Path getBaseDir(){
        return baseDir;
    }

    /**
     * Utility function to initialize the base directory structure
     */
    public void initializeBaseDirectoryStructure(){
        try {
            // Create base dir if it does not exist
            Files.createDirectories(getBaseDir());

            // Create data directories if they does not exist
            List<Path> dataDirectories = Arrays.stream(DATA_CLASS_SUFFIX)
            .map(n -> getBaseDir().resolve(baseKeyspaceName + n+"/"+BY_TIME_DIR))
            .collect(Collectors.toList());
            for(Path dir: dataDirectories){
                Files.createDirectories(dir);
            }

            // create config and meta directory
            Files.createDirectories(getBaseDir().resolve(CONFIG_DIR));
            Files.createDirectories(getBaseDir().resolve(META_DIR));

        } catch (IOException e) {
            LOGGER.error("Unable to initialize base directory structure", e);
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

    /**
     *
     * @param channelName
     * @param start
     * @param end
     * @return  List of List of paths - first list is bin list, second list is because of split
     */
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

    /**
     * Locate the config file for a specific channel based on the standard directory structure
     * @param channelName
     * @return
     */
    public Path locateConfigFile(String channelName){
        Path path = baseDir.resolve("config/"+channelName+"/latest/00000_Config"); // We currently hardcode the version of the deserializer
        if(!Files.exists(path)){
            LOGGER.warn("Unable to locate config file for channel {} at {}", channelName, path);
            return null;
        }
        return path;
    }
}
