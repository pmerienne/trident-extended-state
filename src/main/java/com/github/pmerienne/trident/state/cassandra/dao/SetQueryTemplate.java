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
package com.github.pmerienne.trident.state.cassandra.dao;

import static com.github.pmerienne.trident.state.cassandra.dao.CassandraDao.SET_STATE_TABLE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import storm.trident.state.Serializer;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.google.common.collect.ImmutableMap;

public class SetQueryTemplate extends AbstractQueryTemplate {

	private final Session session;

	private final Map<Operation, PreparedStatement> setPreparedStatements;

	public <T> SetQueryTemplate(Session session) {
		this.session = session;
		this.setPreparedStatements = prepareSetStatements();
	}

	private Map<Operation, PreparedStatement> prepareSetStatements() {
		PreparedStatement setInsert = session.prepare(new SimpleStatement("INSERT INTO " + SET_STATE_TABLE
				+ "(id,value) VALUES(?,?)"));
		PreparedStatement setGetAll = session.prepare(new SimpleStatement("SELECT value FROM " + SET_STATE_TABLE
				+ " WHERE id=? LIMIT 1000000000"));
		PreparedStatement setRemoveAll = session.prepare(new SimpleStatement("DELETE FROM " + SET_STATE_TABLE
				+ " WHERE id=?"));

		return ImmutableMap.of(Operation.INSERT, setInsert, Operation.GET_ALL, setGetAll, Operation.REMOVE_ALL,
				setRemoveAll);
	}

	public <E> void addToSet(String id, E e, Serializer<E> serializer) throws JsonGenerationException,
			JsonMappingException, IOException {
		ByteBuffer serialized = serialize(e, serializer);
		PreparedStatement insertPs = setPreparedStatements.get(Operation.INSERT);
		session.execute(insertPs.bind(id, serialized));
	}

	public <E> void addAllToSet(String id, Collection<? extends E> c, Serializer<E> serializer)
			throws JsonGenerationException, JsonMappingException, IOException {
		PreparedStatement ps = setPreparedStatements.get(Operation.INSERT);
		Iterator<?> iterator = c.iterator();
		BatchStatement batch = new BatchStatement();
		while (iterator.hasNext()) {
			ByteBuffer serialized = serialize((E) iterator.next(), serializer);
			batch.add(ps.bind(id, serialized));
		}
		session.execute(batch);
	}

	public <E> Set<E> getAll(String id, Serializer<E> serializer) throws JsonParseException, JsonMappingException,
			IOException {
		PreparedStatement ps = setPreparedStatements.get(Operation.GET_ALL);
		List<Row> rows = session.execute(ps.bind(id)).all();
		Set<E> result = new HashSet<E>();
		for (Row row : rows) {
			E value = deserialize(row.getBytes("value"), serializer);
			if (Collection.class.isAssignableFrom(value.getClass())) {
				result.addAll((Collection) value);
			} else {
				result.add(value);
			}
		}
		return result;
	}

	public void removeAll(String id) {
		PreparedStatement ps = setPreparedStatements.get(Operation.REMOVE_ALL);
		session.execute(ps.bind(id));
	}
}
