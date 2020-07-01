package ch.psi.daq.imageapi.finder;

public class BaseDir implements Comparable<BaseDir> {
    public String baseDir;
    public String baseKeyspaceName;

    @Override
    public int compareTo(BaseDir x) {
        int n = baseDir.compareTo(x.baseDir);
        if (n != 0) {
            return n;
        }
        else {
            return baseKeyspaceName.compareTo(x.baseKeyspaceName);
        }
    }

    @Override
    public String toString() {
        return String.format("BaseDir { baseDir: %s, baseKeyspaceName: %s }", baseDir, baseKeyspaceName);
    }

}
