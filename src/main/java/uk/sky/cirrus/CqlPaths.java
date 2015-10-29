package uk.sky.cirrus;

import com.google.common.collect.ImmutableSortedMap;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

class CqlPaths {

    private static final String BOOTSTRAP_CQL = "bootstrap.cql";
    private static final FilenameFilter CQL_FILE_NAMES = (file,name) -> name.endsWith(".cql");

    private final ImmutableSortedMap<String, Path> sortedCqlPaths;

    public CqlPaths(Map<String, Path> paths) {
        this.sortedCqlPaths = ImmutableSortedMap.copyOf(paths);
    }

    static CqlPaths create(Collection<Path> directories) {
        final Map<String, Path> paths = new HashMap<>();

        directories.stream()
                .map(path -> path.toFile().listFiles(CQL_FILE_NAMES))
                .map(Arrays::asList)
                .flatMap(Collection::stream)
                .map(File::toPath)
                .forEach(path -> addPathToMap(paths, path));

        return new CqlPaths(paths);
    }

    public void applyInSortedOrder(Function function) {
        sortedCqlPaths.keySet().stream()
                .filter(filename -> !filename.equals(BOOTSTRAP_CQL))
                .forEach(filename -> function.apply(filename, sortedCqlPaths.get(filename)));
    }

    public void applyBootstrap(Function function) {
        function.apply(BOOTSTRAP_CQL, sortedCqlPaths.get(BOOTSTRAP_CQL));
    }

    public interface Function {
        void apply(String filename, Path path);
    }

    private static void addPathToMap(final Map<String, Path> paths, final Path path) {
        final String cqlFileName = path.getFileName().toString();
        if(paths.put(path.getFileName().toString(), path.toAbsolutePath()) != null){
            throw new IllegalArgumentException(String.format("Multiple files with the same name: %s, %s", cqlFileName, path.toAbsolutePath()));
        }
    }
}