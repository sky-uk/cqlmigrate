package uk.sky.cirrus.locking;

import org.assertj.core.api.ThrowableAssert;
import org.joda.time.Duration;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

public class LockConfigBuilderTest {

    @Test
    public void shouldThrowExceptionIfPollingIntervalIsNegative() throws Exception {
        //when
        Throwable throwable = catchThrowable(new ThrowableAssert.ThrowingCallable() {
            @Override
            public void call() throws Throwable {
                LockConfig.builder().withPollingInterval(Duration.millis(-1));
            }
        });

        //then
        assertThat(throwable)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Polling interval must be positive: -1");
    }

    @Test
    public void shouldThrowExceptionIfTimeoutIsNegative() throws Exception {
        //when
        Throwable throwable = catchThrowable(new ThrowableAssert.ThrowingCallable() {
            @Override
            public void call() throws Throwable {
                LockConfig.builder().withTimeout(Duration.millis(-1));
            }
        });

        //then
        assertThat(throwable)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Timeout must be positive: -1");
    }

    @Test
    public void shouldThrowExceptionIfSimpleStrategyIsUsedInCombinationWithDataCenters() throws Exception {
        //when
        Throwable throwable = catchThrowable(new ThrowableAssert.ThrowingCallable() {
            @Override
            public void call() throws Throwable {
                LockConfig.builder().withNetworkTopologyReplication("DC", 1).withSimpleStrategyReplication(1);
            }
        });

        //then
        assertThat(throwable)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Replication class 'SimpleStrategy' cannot be used with data centers: {DC=1}");
    }
}