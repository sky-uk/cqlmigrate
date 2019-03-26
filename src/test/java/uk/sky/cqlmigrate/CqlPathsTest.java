package uk.sky.cqlmigrate;

import com.sun.management.UnixOperatingSystemMXBean;
import org.junit.Before;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;


public class CqlPathsTest {

    private OperatingSystemMXBean os;

    @Before
    public void setUp(){
        os = ManagementFactory.getOperatingSystemMXBean();
    }

    @Test
    public void shouldReleaseFileDescriptorsResources() throws Exception {
        // given
        assumeTrue(os instanceof UnixOperatingSystemMXBean);
        UnixOperatingSystemMXBean unix = (UnixOperatingSystemMXBean) os;
        long openedFileDescriptors = unix.getOpenFileDescriptorCount();
        Path cql_bootstrap = getResourcePath("cql_bootstrap");

        // when
        CqlPaths.create(Collections.singletonList(cql_bootstrap));

        // Then
        assertThat(unix.getOpenFileDescriptorCount(), is(openedFileDescriptors));
    }

    private Path getResourcePath(String resourcePath) throws URISyntaxException {
        return Paths.get(ClassLoader.getSystemResource(resourcePath).toURI());
    }
}
