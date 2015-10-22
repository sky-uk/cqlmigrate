package uk.sky.cirrus;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

class Paths {

    private static final Logger LOGGER = LoggerFactory.getLogger(Paths.class);
    private static final String BOOTSTRAP_CQL = "bootstrap.cql";

    private final Map<String, Path> paths;

    public Paths(Map<String, Path> paths) {
        this.paths = paths;
    }

    static Paths create(Collection<Path> directories) {
        Map<String, Path> paths = new HashMap<>();
        for (Path directory : directories) {
            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory, "*.cql")) {
                for (Path path : directoryStream) {
                    Path previous = paths.put(path.getFileName().toString(), path.toAbsolutePath());
                    if (previous != null) {
                        throw new IllegalArgumentException(String.format("Multiple files with the same name: %s, %s", previous, path.toAbsolutePath()));
                    }
                }
            } catch (Throwable e) {
                LOGGER.error("Failed to locate files: {}", e.getMessage());
                throw Throwables.propagate(e);
            }
        }
        return new Paths(paths);
    }

    public void applyInSortedOrder(Function function) {
        ImmutableSortedMap<String, Path> sortedPaths = ImmutableSortedMap.copyOf(paths);
        for (Map.Entry<String, Path> stringPathEntry : sortedPaths.entrySet()) {
            String filename = stringPathEntry.getKey();
            if (filename.equals(BOOTSTRAP_CQL)) {
                continue;
            }

            Path path = stringPathEntry.getValue();
            function.apply(filename, path);
        }
    }

    public void applyBootstrap(Function function) {
        function.apply(BOOTSTRAP_CQL, paths.get(BOOTSTRAP_CQL));
    }

    public interface Function {
        void apply(String filename, Path path);
    }
}
