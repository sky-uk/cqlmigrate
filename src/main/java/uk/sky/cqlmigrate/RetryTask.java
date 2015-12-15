package uk.sky.cqlmigrate;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

class RetryTask {
    private Callable<Boolean> action;
    private Duration timeout;
    private Duration pollingInterval;

    private RetryTask(Callable<Boolean> action) {
        this.action = action;
    }

    public static RetryTask attempt(Callable<Boolean> action) {
        return new RetryTask(action);
    }

    public RetryTask untilSuccess() throws TimeoutException, InterruptedException {
        checkState(timeout == null, "timeout has not been configured");
        checkState(pollingInterval == null, "polling interval has not been configured");

        long startTime = System.currentTimeMillis();
        try {
            while (!action.call()) {
                if (timedOut(timeout, startTime)) {
                    throw new TimeoutException(String.format("Timed out after waiting %s ms, with timeout %s ms", System.currentTimeMillis() - startTime, timeout.toMillis()));
                }
                Thread.sleep(pollingInterval.toMillis());
            }
        } catch (RuntimeException | TimeoutException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    private void checkState(boolean condition, String message) {
        if (condition) {
           throw new IllegalStateException(message);
        }
    }

    public <R> R thenReturn(Callable<R> result) {
        try {
            return result.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void execute() throws TimeoutException, InterruptedException {
        untilSuccess();
    }

    private static boolean timedOut(Duration timeout, long startTime) {
        long currentDuration = System.currentTimeMillis() - startTime;
        return currentDuration >= timeout.toMillis();
    }

    public RetryTask withTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public RetryTask withPollingInterval(Duration pollingInterval) {
        this.pollingInterval = pollingInterval;
        return this;
    }
}
