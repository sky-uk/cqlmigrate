package uk.sky.cqlmigrate;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import uk.sky.cqlmigrate.exception.CannotAcquireLockException;
import uk.sky.cqlmigrate.exception.CannotReleaseLockException;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class LockTest {

    private static final int POLLING_MILLIS = 100;
    private static final int TIMEOUT_MILLIS = 250;

    private static final LockConfig LOCK_CONFIG = LockConfig.builder()
            .withPollingInterval(Duration.ofMillis(POLLING_MILLIS))
            .withTimeout(Duration.ofMillis(TIMEOUT_MILLIS))
            .build();

    @Mock
    private LockingMechanism lockingMechanism;

    @Test
    public void shouldInitLockingMechanismBeforeAttemptingAcquire() throws Throwable {
        //given
        given(lockingMechanism.acquire(LOCK_CONFIG.getClientId())).willReturn(true);

        //when
        Lock.acquire(lockingMechanism, LOCK_CONFIG);

        //then
        InOrder inOrder = inOrder(lockingMechanism);
        inOrder.verify(lockingMechanism).init();
        inOrder.verify(lockingMechanism).acquire(LOCK_CONFIG.getClientId());
    }

    @Test
    public void ifLockCanBeAcquiredShouldReturnLock() throws Throwable {
        //given
        given(lockingMechanism.acquire(LOCK_CONFIG.getClientId())).willReturn(true);

        //when
        Lock lock = Lock.acquire(lockingMechanism, LOCK_CONFIG);

        //then
        assertThat(lock).isNotNull();
    }

    @Test
    public void retriesToAcquireLockAfterIntervalIfFailedTheFirstTime() throws Throwable {
        //given
        given(lockingMechanism.acquire(LOCK_CONFIG.getClientId())).willReturn(false, true);

        //when
        long startTime = System.currentTimeMillis();
        Lock lock = Lock.acquire(lockingMechanism, LOCK_CONFIG);
        long duration = System.currentTimeMillis() - startTime;

        //then
        assertThat(lock).isNotNull();
        assertThat(duration).isGreaterThan(POLLING_MILLIS);
    }

    @Test
    public void throwsExceptionIfFailedToAcquireLockBeforeTimeout() throws Throwable {
        //given
        given(lockingMechanism.acquire(LOCK_CONFIG.getClientId())).willReturn(false);

        //when
        long startTime = System.currentTimeMillis();
        try {
            Lock.acquire(lockingMechanism, LOCK_CONFIG);
            fail("Expected Exception");
        } catch (CannotAcquireLockException e) {}
        long duration = System.currentTimeMillis() - startTime;

        //then
        assertThat(duration).isLessThanOrEqualTo(TIMEOUT_MILLIS * 2);
    }

    @Test()
    public void throwsExceptionIfThreadSleepIsInterrupted() throws Throwable {
        //given
        given(lockingMechanism.getLockName()).willReturn("some lock");
        given(lockingMechanism.acquire(LOCK_CONFIG.getClientId())).willReturn(false);
        Thread.currentThread().interrupt();

        //when
        Throwable throwable = catchThrowable(() -> Lock.acquire(lockingMechanism, LOCK_CONFIG));

        //then
        assertThat(throwable)
                .isInstanceOf(CannotAcquireLockException.class)
                .hasCauseInstanceOf(InterruptedException.class)
                .hasMessage(String.format("Polling to acquire lock some lock for client %s was interrupted", LOCK_CONFIG.getClientId()));

        //clean up
        assertThat(Thread.interrupted()).isTrue();
    }

    @Test
    public void usesLockingMechanismToReleaseLock() throws Throwable {
        //given
        given(lockingMechanism.acquire(LOCK_CONFIG.getClientId())).willReturn(true);
        given(lockingMechanism.release(LOCK_CONFIG.getClientId())).willReturn(true);
        Lock lock = Lock.acquire(lockingMechanism, LOCK_CONFIG);

        //when
        lock.release();

        //then
        verify(lockingMechanism).release(LOCK_CONFIG.getClientId());
    }

    @Test
    public void retriesToReleaseLockAfterIntervalIfFailedTheFirstTime() throws Throwable {
        //given
        given(lockingMechanism.acquire(LOCK_CONFIG.getClientId())).willReturn(true);
        given(lockingMechanism.release(LOCK_CONFIG.getClientId())).willReturn(false, true);
        Lock lock = Lock.acquire(lockingMechanism, LOCK_CONFIG);

        //when
        long startTime = System.currentTimeMillis();
        lock.release();
        long duration = System.currentTimeMillis() - startTime;

        //then
        assertThat(duration).isGreaterThan(POLLING_MILLIS);
        verify(lockingMechanism, times(2)).release(LOCK_CONFIG.getClientId());
    }

    @Test
    public void throwsExceptionIfFailedToReleaseLockBeforeTimeout() throws Throwable {
        //given
        given(lockingMechanism.acquire(LOCK_CONFIG.getClientId())).willReturn(true);
        given(lockingMechanism.release(LOCK_CONFIG.getClientId())).willReturn(false);
        Lock lock = Lock.acquire(lockingMechanism, LOCK_CONFIG);

        //when
        long startTime = System.currentTimeMillis();
        try {
            lock.release();
            fail("Expected Exception");
        } catch (CannotReleaseLockException e) {
            // nada
        }
        long duration = System.currentTimeMillis() - startTime;

        //then
        assertThat(duration).isLessThanOrEqualTo(TIMEOUT_MILLIS * 2);
    }
}