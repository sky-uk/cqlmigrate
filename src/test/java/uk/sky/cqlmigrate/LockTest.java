package uk.sky.cqlmigrate;

import org.junit.Before;
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
    
    private Lock lock;

    @Before
    public void setUp() throws Exception {
        lock = new Lock(lockingMechanism, LOCK_CONFIG);
    }

    @Test
    public void shouldInitLockingMechanismBeforeAttemptingAcquire() throws Throwable {
        //given
        given(lockingMechanism.acquire(LOCK_CONFIG.getClientId())).willReturn(true);

        //when
        lock.lock();

        //then
        InOrder inOrder = inOrder(lockingMechanism);
        inOrder.verify(lockingMechanism).init();
        inOrder.verify(lockingMechanism).acquire(LOCK_CONFIG.getClientId());
    }

    @Test
    public void ifLockCanBeAcquiredShouldNotThrowException() throws Throwable {
        //given
        given(lockingMechanism.acquire(LOCK_CONFIG.getClientId())).willReturn(true);

        //when
        lock.lock();
    }

    @Test
    public void retriesToAcquireLockAfterIntervalIfFailedTheFirstTime() throws Throwable {
        //given
        given(lockingMechanism.acquire(LOCK_CONFIG.getClientId())).willReturn(false, true);

        //when
        long startTime = System.currentTimeMillis();
        lock.lock();
        long duration = System.currentTimeMillis() - startTime;

        //then
        assertThat(duration).isGreaterThanOrEqualTo(POLLING_MILLIS);
    }

    @Test
    public void throwsExceptionIfFailedToAcquireLockBeforeTimeout() throws Throwable {
        //given
        given(lockingMechanism.acquire(LOCK_CONFIG.getClientId())).willReturn(false);

        //when
        long startTime = System.currentTimeMillis();
        try {
            lock.lock();
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
        Throwable throwable = catchThrowable(() -> lock.lock());

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
        lock.lock();

        //when
        lock.unlock(false);

        //then
        verify(lockingMechanism).release(LOCK_CONFIG.getClientId());
    }

    @Test
    public void doesNotReleaseLockIfMigrationFailed() throws Throwable {
        //given
        given(lockingMechanism.acquire(LOCK_CONFIG.getClientId())).willReturn(true);
        lock.lock();

        //when
        lock.unlock(true);

        //then
        verify(lockingMechanism, never()).release(LOCK_CONFIG.getClientId());
    }

    @Test
    public void releasesLockOnLockIfUnlockOnFailureIsSetToTrue() throws Throwable {
        //given
        LockConfig lockConfig = LockConfig.builder()
                .withPollingInterval(Duration.ofMillis(POLLING_MILLIS))
                .withTimeout(Duration.ofMillis(TIMEOUT_MILLIS))
                .unlockOnFailure()
                .build();

        given(lockingMechanism.acquire(lockConfig.getClientId())).willReturn(true);
        given(lockingMechanism.release(lockConfig.getClientId())).willReturn(true);

        Lock lock = new Lock(lockingMechanism, lockConfig);
        lock.lock();

        //when
        lock.unlock(true);

        //then
        verify(lockingMechanism).release(lockConfig.getClientId());
    }

    @Test
    public void retriesToReleaseLockAfterIntervalIfFailedTheFirstTime() throws Throwable {
        //given
        given(lockingMechanism.acquire(LOCK_CONFIG.getClientId())).willReturn(true);
        given(lockingMechanism.release(LOCK_CONFIG.getClientId())).willReturn(false, true);
        lock.lock();

        //when
        long startTime = System.currentTimeMillis();
        lock.unlock(false);
        long duration = System.currentTimeMillis() - startTime;

        //then
        assertThat(duration).isGreaterThanOrEqualTo(POLLING_MILLIS);
        verify(lockingMechanism, times(2)).release(LOCK_CONFIG.getClientId());
    }

    @Test
    public void throwsExceptionIfFailedToReleaseLockBeforeTimeout() throws Throwable {
        //given
        given(lockingMechanism.acquire(LOCK_CONFIG.getClientId())).willReturn(true);
        given(lockingMechanism.release(LOCK_CONFIG.getClientId())).willReturn(false);
        lock.lock();

        //when
        long startTime = System.currentTimeMillis();
        try {
            lock.unlock(false);
            fail("Expected Exception");
        } catch (CannotReleaseLockException e) {
            // nada
        }
        long duration = System.currentTimeMillis() - startTime;

        //then
        assertThat(duration).isLessThanOrEqualTo(TIMEOUT_MILLIS * 2);
    }
}
