package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(MockitoJUnitRunner.class)
public class CassandraNoOpLockingMechanismTestMock {


    public static final String CLIENT_ID = "CLIENT_ID";
    @Mock
    private CqlSession session;

    private CassandraNoOpLockingMechanism lockingMechanism = new CassandraNoOpLockingMechanism();
    private ExecutorService executorService;


    @Test
    public void shouldPrepareNoInsertLocksQueryWhenInit() throws Throwable {
        //when
        lockingMechanism.init();

        //then
        verifyZeroInteractions(session);

    }

    @Test
    public void nothingShouldInsertLockWhenAcquiringLock() throws Exception {
        //when
        boolean acquire = lockingMechanism.acquire(CLIENT_ID);

        //then
        assertThat(acquire).isTrue();
        verifyZeroInteractions(session);
    }

    @Test
    public void shouldDeleteLockWhenReleasingLock() throws Exception {
        //when
        boolean release = lockingMechanism.release(CLIENT_ID);

        //then
        assertThat(release).isTrue();
        verifyZeroInteractions(session);
    }

    @Test
    public void shouldAlwaysReleasingLock() throws Exception {
        //when
        boolean released = lockingMechanism.release(CLIENT_ID);

        //then
        assertThat(released).isTrue();
    }

    @Test
    public void shouldAlwaysAcquireLock() throws Exception {
        //when
        boolean acquired = lockingMechanism.acquire(CLIENT_ID);

        //then
        assertThat(acquired).isTrue();
    }

}
