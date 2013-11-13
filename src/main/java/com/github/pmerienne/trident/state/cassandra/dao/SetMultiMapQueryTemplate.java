package com.github.pmerienne.trident.state.cassandra.dao;

import static com.github.pmerienne.trident.state.cassandra.dao.CassandraDao.SET_MULTI_MAP_STATE_TABLE;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import storm.trident.state.Serializer;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.github.pmerienne.trident.state.serializer.SerializerFactory;
import com.github.pmerienne.trident.state.serializer.SerializerFactory.SerializerType;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class SetMultiMapQueryTemplate extends AbstractQueryTemplate {

	private final Session session;
	private final Map<Operation, PreparedStatement> preparedStatements;
	private final Serializer<Object> keySerializer = SerializerFactory.createSerializer(SerializerType.BINARY);
	private final HashFunction hf = Hashing.md5();

	public SetMultiMapQueryTemplate(Session session) {
		this.session = session;
		this.preparedStatements = prepareSetMultiMapStatements();
	}

	private Map<Operation, PreparedStatement> prepareSetMultiMapStatements() {
		PreparedStatement setMultiMapCount = session.prepare(new SimpleStatement("SELECT COUNT(*) AS set_count FROM "
				+ SET_MULTI_MAP_STATE_TABLE + " WHERE id=? and key=?"));
		PreparedStatement setMultiMapInsert = session.prepare(new SimpleStatement("INSERT INTO "
				+ SET_MULTI_MAP_STATE_TABLE + "(id,key,column,value) VALUES(?,?,?,?)"));
		PreparedStatement setMultiMapGet = session.prepare(new SimpleStatement("SELECT value FROM "
				+ SET_MULTI_MAP_STATE_TABLE + " WHERE id=? AND key=? LIMIT " + Integer.MAX_VALUE));
		return ImmutableMap.of(Operation.COUNT, setMultiMapCount, Operation.INSERT_COLUMN, setMultiMapInsert,
				Operation.GET_ALL, setMultiMapGet);
	}

	public <K, V> void put(String id, K key, V value, Serializer<V> serializer) {
		ByteBuffer serializedKey = serialize(key, keySerializer);
		ByteBuffer serializedValue = serialize(value, serializer);
		ByteBuffer column = ByteBuffer.wrap(hf.hashBytes(serializedValue.array()).asBytes());

		PreparedStatement ps = preparedStatements.get(Operation.INSERT_COLUMN);
		session.execute(ps.bind(id, serializedKey, column, serializedValue));
	}

	public <K> long sizeOf(String id, K key) {
		long count = 0;
		ByteBuffer serializedKey = serialize(key, keySerializer);
		PreparedStatement ps = preparedStatements.get(Operation.COUNT);

		Row row = session.execute(ps.bind(id, serializedKey)).one();
		if (row != null) {
			count = row.getLong("set_count");
		}
		return count;
	}

	public <K, V> Set<V> get(String id, K key, Serializer<V> serializer) {
		Set<V> result = new HashSet<V>();
		ByteBuffer serializedKey = serialize(key, keySerializer);
		PreparedStatement ps = preparedStatements.get(Operation.GET_ALL);

		List<Row> rows = session.execute(ps.bind(id, serializedKey)).all();
		for (Row row : rows) {
			ByteBuffer rawValue = row.getBytes("value");
			result.add(deserialize(rawValue, serializer));
		}
		return result;
	}
}
