package uk.sky.cqlmigrate;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.sky.cqlmigrate.ChecksumCalculator.calculateChecksum;

public class ChecksumCalculatorTest {

    @Test
    public void calculateChecksumThrowsWhenFileDoesNotExist() throws Exception {
        // given
        Path path = Paths.get("/non-existant-file.cql");

        // When
        RuntimeException runtimeException = Assertions.catchThrowableOfType(() -> calculateChecksum(path), RuntimeException.class);

        // then
        assertThat(runtimeException).isNotNull()
                .hasCauseInstanceOf(NoSuchFileException.class);
    }

    @Test
    public void calculateChecksumReturnsChecksumForBootstrapFile() throws Exception {
        // given
        Path path = Paths.get(ClassLoader.getSystemResource("cql_valid_one/bootstrap.cql").toURI());

        // When
        String checksum = calculateChecksum(path);

        // then
        assertThat(checksum).isEqualTo("1918e9ddab34fa89b2a01b777d41eb94cae8de07");
    }

    @Test
    public void calculateChecksumReturnsChecksumForStandardCqlFile() throws Exception {
        // given
        Path path = Paths.get(ClassLoader.getSystemResource("cql_valid_one/2015-04-01-13:56-create-status-table.cql").toURI());

        // When
        String checksum = calculateChecksum(path);

        // then
        assertThat(checksum).isEqualTo("fa03a30eab18b64b74ee1ea7816e0513f03b4ac7");
    }

}