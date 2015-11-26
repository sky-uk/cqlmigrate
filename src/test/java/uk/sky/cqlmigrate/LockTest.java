package uk.sky.cqlmigrate;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import uk.sky.cqlmigrate.Lock;
import uk.sky.cqlmigrate.LockConfig;
import uk.sky.cqlmigrate.LockingMechanism;
import uk.sky.cqlmigrate.exception.CannotAcquireLockException;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;

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
        given(lockingMechanism.acquire()).willReturn(true);

        //when
        Lock.acquire(lockingMechanism, LOCK_CONFIG);

        //then
        InOrder inOrder = inOrder(lockingMechanism);
        inOrder.verify(lockingMechanism).init();
        inOrder.verify(lockingMechanism).acquire();
    }

    @Test
    public void ifLockCanBeAcquiredShouldReturnLock() throws Throwable {
        //given
        given(lockingMechanism.acquire()).willReturn(true);

        //when
        Lock lock = Lock.acquire(lockingMechanism, LOCK_CONFIG);

        //then
        assertThat(lock).isNotNull();
    }

    @Test
    public void retriesToAcquireLockAfterIntervalIfFailedTheFirstTime() throws Throwable {
        //given
        given(lockingMechanism.acquire()).willReturn(false, true);

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
        given(lockingMechanism.acquire()).willReturn(false);

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

    @Test
    public void usesLockingMechanismToReleaseLock() throws Throwable {
        //given
        given(lockingMechanism.acquire()).willReturn(true);
        Lock lock = Lock.acquire(lockingMechanism, LOCK_CONFIG);

        //when
        lock.release();

        //then
        verify(lockingMechanism).release();
    }
}