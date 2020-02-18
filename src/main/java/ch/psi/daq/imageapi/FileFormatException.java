package ch.psi.daq.imageapi;

import java.nio.file.Path;

public class FileFormatException extends Exception {

    Path path;
    long position;

    public FileFormatException(String msg, Path path, long position) {
        super(String.format("%s  path: %s  position: %d", msg, path.toString(), position));
        this.path = path;
        this.position = position;
    }

}
