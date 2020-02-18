package ch.psi.daq.imageapi3;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class FileManagerTest implements IFileManager {

    public static FileManagerTest create() {
        FileManagerTest ret = new FileManagerTest();
        return ret;
    }

    @Override
    public Collection<String> getChannelNames() {
        return Arrays.asList("SARES20-CAMS142-M5:FPICTURE");
    }

    @Override
    public List<List<Path>> locateDataFiles(String channelName, Instant begin, Instant end) {
        return Arrays.asList(Arrays.asList(Path.of("/home/werder_d/test_data_imagebuffer/0000000000003600000_00000_Data")));
    }

    @Override
    public Path locateConfigFile(String channelName) { return null; }

}
