package uk.sky.cqlmigrate;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class CassandraNoOpLockingMechanismTest {

    public static final String CLIENT_ID = "CLIENT_ID";

    private final CassandraNoOpLockingMechanism lockingMechanism = new CassandraNoOpLockingMechanism();

    @Test
    public void shouldDoNothingWhenInit() throws Throwable {
        //when
        lockingMechanism.init();
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
