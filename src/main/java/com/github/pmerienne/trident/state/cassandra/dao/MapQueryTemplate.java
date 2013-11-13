package com.github.pmerienne.trident.state.cassandra.dao;

import static com.github.pmerienne.trident.state.cassandra.dao.CassandraDao.MAP_STATE_TABLE;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import storm.trident.state.Serializer;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.google.common.collect.ImmutableMap;

public class MapQueryTemplate extends AbstractQueryTemplate {

	private final Session session;
	private final Map<Operation, PreparedStatement> mapPreparedStatements;

	public MapQueryTemplate(Session session) {
		this.session = session;
		this.mapPreparedStatements = prepareMapStatements();
	}

	private Map<Operation, PreparedStatement> prepareMapStatements() {
		PreparedStatement mapInsert = session.prepare(new SimpleStatement("INSERT INTO " + MAP_STATE_TABLE
				+ "(id,map_key,value) VALUES(?,?,?)"));
		PreparedStatement mapGet = session.prepare(new SimpleStatement("SELECT value FROM " + MAP_STATE_TABLE
				+ " WHERE id=? AND map_key IN ?"));
		return ImmutableMap.of(Operation.INSERT, mapInsert, Operation.GET, mapGet);
	}

	public <E> List<E> multiGet(String id, List<String> keys, Serializer<E> serializer) {
		PreparedStatement ps = mapPreparedStatements.get(Operation.GET);
		List<Row> rows = session.execute(ps.bind(id, keys)).all();
		List<E> result = new ArrayList<E>();
		for (Row row : rows) {
			result.add(deserialize(row.getBytes("value"), serializer));
		}
		return result;
	}

	public <E> void multiPut(String id, List<String> keys, List<E> values, Serializer<E> serializer) {
		PreparedStatement ps = mapPreparedStatements.get(Operation.INSERT);
		BatchStatement batch = new BatchStatement();
		List<ByteBuffer> serializedValues = new ArrayList<ByteBuffer>();

		for (E value : values) {
			serializedValues.add(serialize(value, serializer));
		}

		for (int i = 0; i < keys.size(); i++) {
			String key = keys.get(i);
			ByteBuffer value = serializedValues.get(i);
			batch.add(ps.bind(id, key, value));
		}

		session.execute(batch);
	}
}
