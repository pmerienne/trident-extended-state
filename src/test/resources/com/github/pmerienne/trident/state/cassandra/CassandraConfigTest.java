package com.github.pmerienne.trident.state.cassandra;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.github.pmerienne.trident.state.cassandra.dao.CassandraDao;
import com.github.pmerienne.trident.state.cassandra.embedded.CQLResource;
import com.github.pmerienne.trident.state.cassandra.embedded.CQLResourceBuilder;

@RunWith(MockitoJUnitRunner.class)
public class CassandraConfigTest {

    @Rule
    public CQLResource resource = CQLResourceBuilder.noTruncate();

    @Test
    public void should_bootstrap_cassandra_config() throws Exception {

        CassandraConfig config = CassandraConfig.builder()
                .withContactPoints("localhost")
                .withPort(resource.getCQLPort())
                .withKeyspace("trident_ex_state").build();

        CassandraDao cassandraDao = config.getDao();
        assertThat(cassandraDao).isNotNull();
    }
}