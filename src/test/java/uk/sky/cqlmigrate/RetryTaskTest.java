package uk.sky.cqlmigrate;

import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@SuppressWarnings("unchecked")
public class RetryTaskTest {

    private RetryTask retryTask;
    private Callable<Boolean> aTask;

    @Before
    public void setup() {
        aTask = (Callable<Boolean>) mock(Callable.class);

        retryTask = RetryTask.attempt(aTask)
                .withPollingInterval(Duration.ofSeconds(10))
                .withTimeout(Duration.ofSeconds(10));
    }

    @Test
    public void shouldReturnValueIfPredicateIsSuccessful() throws Throwable {
        //given
        String expectedResult = "Success";
        given(aTask.call()).willReturn(true);

        //when
        String result = retryTask.untilSuccess().thenReturn(() -> "Success");

        //then
        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCompleteIfPredicateIsSuccessful() throws Throwable {
        //given
        given(aTask.call()).willReturn(true);

        retryTask = RetryTask.attempt(aTask)
                .withPollingInterval(Duration.ofSeconds(10))
                .withTimeout(Duration.ofSeconds(10));

        //when
        retryTask.execute();

        //then
        verify(aTask).call();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldRetryAsManyTimesAsNecessaryUntilSuccess() throws Throwable {
        //given
        given(aTask.call()).willReturn(false, false, true);

        RetryTask retryTask = RetryTask.attempt(aTask)
                .withPollingInterval(Duration.ofMillis(5))
                .withTimeout(Duration.ofSeconds(10));

        //when
        retryTask.untilSuccess();

        //then
        verify(aTask, times(3)).call();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldWaitConfiguredPollingIntervalBeforeRetrying() throws Throwable {
        //given
        given(aTask.call()).willReturn(false, false, true);

        RetryTask retryTask = RetryTask.attempt(aTask)
                .withPollingInterval(Duration.ofMillis(5))
                .withTimeout(Duration.ofSeconds(10));

        Instant approximateStartTime = Instant.now();

        //when
        retryTask.untilSuccess();
        Instant approximateEndTime = Instant.now();

        //then
        assertThat(Duration.between(approximateStartTime, approximateEndTime)).isGreaterThanOrEqualTo(Duration.ofMillis(5 * 2));
    }

    @Test
    public void shouldTimeoutIfElapsedTimeGoesBeyondConfiguredValue() throws Throwable {
        //given
        Callable<Boolean> aTask = () -> {
            Thread.sleep(3);
            return false;
        };

        RetryTask retryTask = RetryTask.attempt(aTask)
                .withPollingInterval(Duration.ofMillis(5))
                .withTimeout(Duration.ofMillis(6));

        //when

        Throwable throwable = catchThrowable(retryTask::execute);

        //then
        assertThat(throwable)
                .isInstanceOf(TimeoutException.class)
                .hasMessageStartingWith("Timed out after waiting ")
                .hasMessageEndingWith(" ms, with timeout 6 ms");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldThrowInterruptedExceptionIfInterruptedWhileWaitingThePollingInterval() throws Throwable {
        //given
        given(aTask.call()).willReturn(false, true);

        RetryTask retryTask = RetryTask.attempt(aTask)
                .withPollingInterval(Duration.ofMillis(5))
                .withTimeout(Duration.ofMillis(6));

        Thread.currentThread().interrupt();

        //when
        Throwable throwable = catchThrowable(retryTask::execute);

        //then
        assertThat(throwable).isInstanceOf(InterruptedException.class);
    }

    @Test
    public void shouldWrapCheckedExceptionsFromExecutionInRuntimeException() throws Throwable {
        //given
        Callable<Boolean> aTask = () ->  { throw new FileNotFoundException("Dummy checked exception"); };

        RetryTask retryTask = RetryTask.attempt(aTask)
                .withPollingInterval(Duration.ofMillis(5))
                .withTimeout(Duration.ofMillis(6));
        //when
        Throwable throwable = catchThrowable(retryTask::execute);

        //then
        assertThat(throwable)
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(FileNotFoundException.class);
    }

    @Test
    public void checksThatTimeoutIsSet() throws Throwable {
        //given
        RetryTask retryTask = RetryTask.attempt(() -> true).withPollingInterval(Duration.ofHours(1));

        //when
        Throwable throwable = catchThrowable(retryTask::execute);

        //then
        assertThat(throwable)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("timeout has not been configured");
    }

    @Test
    public void checksThatPollingIntervalIsSet() throws Throwable {
        //given
        RetryTask retryTask = RetryTask.attempt(() -> true).withTimeout(Duration.ofHours(1));

        //when
        Throwable throwable = catchThrowable(retryTask::execute);

        //then
        assertThat(throwable)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("polling interval has not been configured");
    }


}