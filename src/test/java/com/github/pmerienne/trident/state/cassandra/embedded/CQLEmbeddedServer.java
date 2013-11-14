/**
 * Copyright 2013-2015 Pierre Merienne
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.pmerienne.trident.state.cassandra.embedded;

import static com.datastax.driver.core.ProtocolOptions.Compression.SNAPPY;
import static com.github.pmerienne.trident.state.cassandra.embedded.CassandraEmbeddedConfigParameters.*;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.policies.Policies;
import com.github.pmerienne.trident.state.cassandra.dao.CassandraDao;

public class CQLEmbeddedServer extends AbstractEmbeddedServer {
	private static final Object SEMAPHORE = new Object();

	private static final Logger LOGGER = LoggerFactory.getLogger(CQLEmbeddedServer.class);

	private static boolean initialized = false;

	private static Session session;

	public CQLEmbeddedServer(Map<String, Object> originalParameters) {
		synchronized (SEMAPHORE) {
			if (!initialized) {
				Map<String, Object> parameters = CassandraEmbeddedConfigParameters
						.mergeWithDefaultParameters(originalParameters);
				startServer(parameters);
				initialize(parameters);
			}
		}
	}

	public Session getSession() {
		return session;
	}

	private void initialize(Map<String, Object> parameters) {

		String keyspaceName = extractAndValidateKeyspaceName(parameters);
		Boolean keyspaceDurableWrite = (Boolean) parameters.get(KEYSPACE_DURABLE_WRITE);

		String hostname;
		int cqlPort;

		String cassandraHost = System.getProperty(CASSANDRA_HOST);
		if (StringUtils.isNotBlank(cassandraHost) && cassandraHost.contains(":")) {
			String[] split = cassandraHost.split(":");
			hostname = split[0];
			cqlPort = Integer.parseInt(split[1]);
		} else {
			hostname = DEFAULT_CASSANDRA_HOST;
			cqlPort = (Integer) parameters.get(CASSANDRA_CQL_PORT);
		}

		Cluster cluster = createCluster(hostname, cqlPort);
		createKeyspaceIfNeeded(cluster, keyspaceName, keyspaceDurableWrite);
		session = cluster.connect(keyspaceName);
		CassandraDao.createTableIfNotExists(session);
		initialized = true;
	}

	private Cluster createCluster(String host, int cqlPort) {
		return Cluster.builder().addContactPoint(host).withPort(cqlPort).withCompression(SNAPPY)
				.withLoadBalancingPolicy(Policies.defaultLoadBalancingPolicy())
				.withRetryPolicy(Policies.defaultRetryPolicy())
				.withReconnectionPolicy(Policies.defaultReconnectionPolicy()).build();
	}

	private void createKeyspaceIfNeeded(Cluster cluster, String keyspaceName, Boolean keyspaceDurableWrite) {
		final Session session = cluster.connect("system");
		final Row row = session.execute(
				"SELECT count(1) FROM schema_keyspaces WHERE keyspace_name='" + keyspaceName + "'").one();
		if (row.getLong(0) != 1) {
			StringBuilder createKeyspaceStatement = new StringBuilder("CREATE keyspace ");
			createKeyspaceStatement.append(keyspaceName);
			createKeyspaceStatement.append(" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1}");
			if (!keyspaceDurableWrite) {
				createKeyspaceStatement.append(" AND DURABLE_WRITES=false");
			}
			session.execute(createKeyspaceStatement.toString());
		}
		session.shutdown();
	}

	public void truncateTable(String tableName) {
		String query = "TRUNCATE " + tableName;
		session.execute(new SimpleStatement(query).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL));
		LOGGER.debug("{} : [{}] with CONSISTENCY LEVEL [ALL]", "Simple query", query);
	}
}
