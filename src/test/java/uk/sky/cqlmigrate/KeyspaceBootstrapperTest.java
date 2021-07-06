package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.mockito.Mockito.*;

public class KeyspaceBootstrapperTest {

    private static final String KEYSPACE = "keyspace";

    private final SessionContext sessionContext = mock(SessionContext.class);
    private final CqlPaths paths = mock(CqlPaths.class);
    private final CqlSession cqlSession = mock(CqlSession.class);

    private KeyspaceBootstrapper keyspaceBootstrapper;

    @Before
    public void setUp() {
        when(sessionContext.getSession()).thenReturn(cqlSession);
        keyspaceBootstrapper = new KeyspaceBootstrapper(sessionContext, KEYSPACE, paths);
    }

    @Test
    public void checkBootstrapLoadsFilesSuccessfullyIfKeyspaceNotFound() {
        // given
        Metadata metadata = mock(Metadata.class);
        when(cqlSession.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspace(KEYSPACE)).thenReturn(Optional.empty());

        // when
        keyspaceBootstrapper.bootstrap();

        // then
        verify(paths, times(1)).applyBootstrap(any());
    }

    @Test
    public void checkBootstrapNotLoadsFilesIfKeyspaceFound() {
        // given
        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(cqlSession.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspace(KEYSPACE)).thenReturn(Optional.of(keyspaceMetadata));

        // when
        keyspaceBootstrapper.bootstrap();

        // then
        verify(paths, times(0)).applyBootstrap(any());
    }
}
