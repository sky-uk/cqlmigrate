package uk.sky.cirrus.locking;

import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

public class LockConfigBuilderTest {

    @Test
    public void shouldThrowExceptionIfPollingIntervalIsNegative() throws Exception {
        //when
        Throwable throwable = catchThrowable(() -> LockConfig.builder().withPollingInterval(Duration.ofMillis(-1)));

        //then
        assertThat(throwable)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Polling interval must be positive: -1");
    }

    @Test
    public void shouldThrowExceptionIfTimeoutIsNegative() throws Exception {
        //when
        Throwable throwable = catchThrowable(() -> LockConfig.builder().withTimeout(Duration.ofMillis(-1)));

        //then
        assertThat(throwable)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Timeout must be positive: -1");
    }

    @Test
    public void shouldThrowExceptionIfSimpleStrategyIsUsedInCombinationWithDataCenters() throws Exception {
        //when
        Throwable throwable = catchThrowable(() -> LockConfig.builder().withNetworkTopologyReplication("DC", 1).withSimpleStrategyReplication(1));

        //then
        assertThat(throwable)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Replication class 'SimpleStrategy' cannot be used with data centers: {DC=1}");
    }
}