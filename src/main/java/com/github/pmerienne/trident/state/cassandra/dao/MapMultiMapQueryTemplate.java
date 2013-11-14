package com.github.pmerienne.trident.state.cassandra.dao;

import static com.github.pmerienne.trident.state.cassandra.dao.CassandraDao.MAP_MULTI_MAP_STATE_TABLE;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.state.Serializer;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.github.pmerienne.trident.state.serializer.SerializerFactory;
import com.github.pmerienne.trident.state.serializer.SerializerFactory.SerializerType;
import com.google.common.collect.ImmutableMap;

public class MapMultiMapQueryTemplate extends AbstractQueryTemplate {

	private final Session session;
	private final Map<Operation, PreparedStatement> preparedStatements;
	private final Serializer<Object> keySerializer = SerializerFactory.createSerializer(SerializerType.BINARY);

	public MapMultiMapQueryTemplate(Session session) {
		this.session = session;
		this.preparedStatements = prepareMapMultiMapStatements();
	}

	private Map<Operation, PreparedStatement> prepareMapMultiMapStatements() {
		PreparedStatement mapMultiMapInsert = session.prepare(new SimpleStatement("INSERT INTO "
				+ MAP_MULTI_MAP_STATE_TABLE + "(id,outer_key,inner_key,value) VALUES(?,?,?,?)"));
		PreparedStatement mapMultiMapGet = session.prepare(new SimpleStatement("SELECT value FROM "
				+ MAP_MULTI_MAP_STATE_TABLE + " WHERE id=? AND outer_key=? AND inner_key=?"));
		PreparedStatement mapMultiMapGetAll = session.prepare(new SimpleStatement("SELECT inner_key,value FROM "
				+ MAP_MULTI_MAP_STATE_TABLE + " WHERE id=? AND outer_key=?"));
		return ImmutableMap.of(Operation.INSERT_COLUMN, mapMultiMapInsert, Operation.GET, mapMultiMapGet,
				Operation.GET_ALL, mapMultiMapGetAll);
	}

	public <K1, K2, V> void put(String id, K1 key1, K2 key2, V value, Serializer<K2> subKeySerializer,
			Serializer<V> serializer) {
		ByteBuffer serializedK1 = serialize(key1, keySerializer);
		ByteBuffer serializedK2 = serialize(key2, subKeySerializer);
		ByteBuffer serializedValue = serialize(value, serializer);

		PreparedStatement ps = preparedStatements.get(Operation.INSERT_COLUMN);
		session.execute(ps.bind(id, serializedK1, serializedK2, serializedValue));
	}

	public <K1, K2, V> V get(String id, K1 key1, K2 key2, Serializer<K2> subKeySerializer, Serializer<V> serializer) {
		V result = null;
		ByteBuffer serializedK1 = serialize(key1, keySerializer);
		ByteBuffer serializedK2 = serialize(key2, subKeySerializer);

		PreparedStatement ps = preparedStatements.get(Operation.GET);
		Row row = session.execute(ps.bind(id, serializedK1, serializedK2)).one();

		if (row != null) {
			result = deserialize(row.getBytes("value"), serializer);
		}
		return result;
	}

	public <K1, K2, V> Map<K2, V> getAll(String id, K1 key1, Serializer<K2> subKeySerializer, Serializer<V> serializer) {
		Map<K2, V> result = new HashMap<K2, V>();
		ByteBuffer serializedK1 = serialize(key1, keySerializer);

		PreparedStatement ps = preparedStatements.get(Operation.GET_ALL);
		List<Row> rows = session.execute(ps.bind(id, serializedK1)).all();
		for (Row row : rows) {
			K2 subKey = deserialize(row.getBytes("inner_key"), subKeySerializer);
			V value = deserialize(row.getBytes("value"), serializer);
			result.put(subKey, value);
		}
		return result;
	}
}
