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
package com.github.pmerienne.trident.state.redis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import redis.clients.jedis.Jedis;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.state.ExtendedStateFactory;
import com.github.pmerienne.trident.state.SparseMatrixState;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class RedisSparseMatrixState<T> extends AbstractRedisState<T> implements SparseMatrixState<T> {

	private final static String COLUMN_KEY = "column";
	private final static String ROW_KEY = "row";

	public RedisSparseMatrixState(String id) {
		super(id);
	}

	public RedisSparseMatrixState(String id, RedisConfig config) {
		super(id, config);
	}

	@Override
	public T get(long i, long j) {
		Jedis jedis = this.pool.getResource();
		String rowKey = this.getRowKey(j);
		String serializedValue = jedis.hget(rowKey, Long.toString(i));

		if (StringUtils.isBlank(serializedValue)) {
			return null;
		}

		T value = this.serializer.deserialize(serializedValue.getBytes());
		return value;
	}

	@Override
	public void set(long i, long j, T value) {
		String columnKey = this.getColumnKey(i);
		String rowKey = this.getRowKey(j);

		this.set(rowKey, i, value);
		this.set(columnKey, j, value);
	}

	protected void set(String key, long index, T value) {
		Jedis jedis = this.pool.getResource();
		String field = Long.toString(index);

		if (value != null) {
			byte[] serializedValue = this.serializer.serialize(value);
			jedis.hset(key, field, new String(serializedValue));
		} else {
			jedis.hdel(key, field);
		}
	}

	@Override
	public SparseVector<T> getColumn(long i) {
		Jedis jedis = this.pool.getResource();

		String columnKey = this.getColumnKey(i);

		Map<String, String> serializedResults = jedis.hgetAll(columnKey);

		SparseVector<T> column = new RedisSparseVector<T>(serializedResults, serializer);
		return column;
	}

	@Override
	public void setColumn(long i, SparseVector<T> column) {
		Jedis jedis = this.pool.getResource();
		String columnKey = this.getColumnKey(i);

		jedis.del(columnKey);

		Map<String, String> values = new HashMap<String, String>();
		for (long index : column.indexes()) {
			values.put(Long.toString(index), new String(this.serializer.serialize(column.get(index))));
		}

		jedis.hmset(columnKey, values);
	}

	@Override
	public SparseVector<T> getRow(long j) {
		Jedis jedis = this.pool.getResource();

		String rowKey = this.getRowKey(j);

		Map<String, String> serializedResults = jedis.hgetAll(rowKey);

		SparseVector<T> row = new RedisSparseVector<T>(serializedResults, serializer);
		return row;
	}

	@Override
	public void setRow(long j, SparseVector<T> row) {
		Jedis jedis = this.pool.getResource();
		String rowKey = this.getRowKey(j);

		jedis.del(rowKey);

		Map<String, String> values = new HashMap<String, String>();
		for (long index : row.indexes()) {
			values.put(Long.toString(index), new String(this.serializer.serialize(row.get(index))));
		}

		jedis.hmset(rowKey, values);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected String getColumnKey(long i) {
		List keys = Arrays.asList(COLUMN_KEY, Long.toString(i));
		return this.generateKey(keys);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected String getRowKey(long j) {
		List keys = Arrays.asList(ROW_KEY, Long.toString(j));
		return this.generateKey(keys);
	}

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
	}

	public static class Factory<T> implements ExtendedStateFactory<RedisSparseMatrixState<T>> {

		private static final long serialVersionUID = 4718043951532492603L;

		private final String id;

		public Factory() {
			this.id = UUID.randomUUID().toString();
		}

		public Factory(String id) {
			this.id = id;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			State state = new RedisSparseMatrixState(this.id, new RedisConfig(conf));
			return state;
		}
	}

	protected static class RedisSparseVector<T> implements SparseVector<T> {

		private static final long serialVersionUID = 3559058694806143009L;

		private Map<String, String> values = new HashMap<String, String>();
		protected Serializer<T> serializer;

		public RedisSparseVector() {
		}

		public RedisSparseVector(Map<String, String> values, Serializer<T> serializer) {
			this.values = values;
			this.serializer = serializer;
		}

		@Override
		public T get(long i) {
			String serializedValue = this.values.get(Long.toString(i));
			if (StringUtils.isBlank(serializedValue)) {
				return null;
			} else {
				return this.serializer.deserialize(serializedValue.getBytes());
			}
		}

		@Override
		public void set(long i, T value) {
			String key = Long.toString(i);

			if (value != null) {
				String serializedValue = new String(this.serializer.serialize(value));
				this.values.put(key, serializedValue);
			} else {
				this.values.remove(key);
			}
		}

		@Override
		public Set<Long> indexes() {
			return Sets.newTreeSet(Iterables.transform(this.values.keySet(), new Function<String, Long>() {
				@Override
				public Long apply(String string) {
					return Long.parseLong(string);
				}

			}));
		}
	}

}
