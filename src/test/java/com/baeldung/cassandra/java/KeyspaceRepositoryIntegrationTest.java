package com.baeldung.cassandra.java;

import com.baeldung.cassandra.java.client.CassandraConnector;
import com.baeldung.cassandra.java.client.repository.KeyspaceRepository;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class KeyspaceRepositoryIntegrationTest {
    private Logger logger = LoggerFactory.getLogger(KeyspaceRepositoryIntegrationTest.class);

    private CassandraConnector client;

    private Session session;

    private KeyspaceRepository schemaRepository;

    @Before
    public void init() throws ConfigurationException, InterruptedException {
        // Start an embedded Cassandra Server
        try {
            try {
                EmbeddedCassandraServerHelper.startEmbeddedCassandra(10000);
            } catch (ConfigurationException e) {
                logger.error("Could not start Embedded Cassandra Server");
                e.printStackTrace();
                throw e;
            }

            client = new CassandraConnector();
            client.connect("127.0.0.1", 9042);
            session = client.getSession();

        } catch (TTransportException | IOException e) {
            e.printStackTrace();
        }

        schemaRepository = new KeyspaceRepository(session);
    }

    @Test
    public void whenCreatingAKeyspace_thenCreated() {
        String keyspaceName = "testBaeldungKeyspace";
        schemaRepository.createKeyspace(keyspaceName, "SimpleStrategy", 1);

        // ResultSet result = session.execute("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'testBaeldungKeyspace';");

        ResultSet result = session.execute("SELECT * FROM system_schema.keyspaces;");

        // Check if the Keyspace exists in the returned keyspaces.
        List<String> matchedKeyspaces = result.all().stream().filter(r -> r.getString(0).equals(keyspaceName.toLowerCase())).map(r -> r.getString(0)).collect(Collectors.toList());
        assertEquals(matchedKeyspaces.size(), 1);
        assertTrue(matchedKeyspaces.get(0).equals(keyspaceName.toLowerCase()));
    }

    @Test
    public void whenDeletingAKeyspace_thenDoesNotExist() {
        String keyspaceName = "testBaeldungKeyspace";

        // schemaRepository.createKeyspace(keyspaceName, "SimpleStrategy", 1);
        schemaRepository.deleteKeyspace(keyspaceName);

        ResultSet result = session.execute("SELECT * FROM system_schema.keyspaces;");
        boolean isKeyspaceCreated = result.all().stream().anyMatch(r -> r.getString(0).equals(keyspaceName.toLowerCase()));
        assertFalse(isKeyspaceCreated);
    }

    @After
    public void cleanup() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        client.close();
    }
}