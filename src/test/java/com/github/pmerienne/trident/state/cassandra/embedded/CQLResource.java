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

import static com.github.pmerienne.trident.state.cassandra.dao.CassandraDao.*;
import static com.github.pmerienne.trident.state.cassandra.embedded.CassandraEmbeddedConfigParameters.*;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

import java.lang.instrument.Instrumentation;

import org.github.jamm.MemoryMeter;

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

public class CQLResource extends AbstractTestResource {

	private static final String[] tablesToTruncate = new String[] { SET_STATE_TABLE, MAP_STATE_TABLE,
			SET_MULTI_MAP_STATE_TABLE, MAP_MULTI_MAP_STATE_TABLE, SPARSE_MATRIX_ROW_STATE_TABLE,
			SPARSE_MATRIX_COLUMN_STATE_TABLE };

	private final CQLEmbeddedServer server;

	private final Session session;

	/**
	 * Initialize a new embedded Cassandra server
	 * 
	 * @param cleanUpSteps
	 *            when to truncate tables for clean up. Possible values are :
	 *            Steps.BEFORE_TEST, Steps.AFTER_TEST and Steps.BOTH (Default
	 *            value)
	 */
	CQLResource(Steps cleanUpSteps) {
		super(cleanUpSteps, tablesToTruncate);

		final ImmutableMap<String, Object> config = ImmutableMap.<String, Object> of(CLEAN_CASSANDRA_DATA_FILES, true,
				KEYSPACE_NAME, DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME, KEYSPACE_DURABLE_WRITE, false);

		setUpMemoryMeter();
		server = new CQLEmbeddedServer(config);
		session = server.getSession();
	}

	/**
	 * Initialize a new embedded Cassandra server
	 * 
	 */
	CQLResource() {
		super();
		final ImmutableMap<String, Object> config = ImmutableMap.<String, Object> of(CLEAN_CASSANDRA_DATA_FILES, true,
				KEYSPACE_NAME, DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME, KEYSPACE_DURABLE_WRITE, false);
		setUpMemoryMeter();
		server = new CQLEmbeddedServer(config);
		session = server.getSession();
	}

	/**
	 * Return a native CQL3 Session
	 * 
	 * @return native CQL3 Session
	 */
	public Session getNativeSession() {
		return session;
	}

	private void setUpMemoryMeter() {
		Instrumentation inst = mock(Instrumentation.class);
		when(inst.getObjectSize(anyObject())).thenReturn(100L);

		MemoryMeter.premain("", inst);
	}

	@Override
	protected void truncateTables() {
		if (tables != null) {
			for (String table : tables) {
				server.truncateTable(table);
			}
		}
	}

}
