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

	private final static String DEFAULT_COLUMN_KEY = "column";
	private final static String DEFAULT_ROW_KEY = "row";

	private final String columnKey;
	private final String rowKey;

	public RedisSparseMatrixState(String id) {
		super(id);
		this.columnKey = DEFAULT_COLUMN_KEY;
		this.rowKey = DEFAULT_ROW_KEY;
	}

	public RedisSparseMatrixState(String id, String columnKey, String rowKey) {
		super(id);
		this.columnKey = columnKey;
		this.rowKey = rowKey;
	}

	public RedisSparseMatrixState(String id, RedisConfig config) {
		super(id, config);
		this.columnKey = DEFAULT_COLUMN_KEY;
		this.rowKey = DEFAULT_ROW_KEY;
	}

	public RedisSparseMatrixState(String id, RedisConfig config, String columnKey, String rowKey) {
		super(id, config);
		this.columnKey = columnKey;
		this.rowKey = rowKey;
	}

	@Override
	public T get(long i, long j) {
		Jedis jedis = this.pool.getResource();
		try {
			byte[] rowKey = this.getRowKey(j);
			byte[] serializedValue = jedis.hget(rowKey, Long.toString(i).getBytes());

			if (serializedValue == null) {
				return null;
			}

			T value = this.serializer.deserialize(serializedValue);
			return value;
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public void set(long i, long j, T value) {
		byte[] columnKey = this.getColumnKey(i);
		byte[] rowKey = this.getRowKey(j);

		this.set(rowKey, i, value);
		this.set(columnKey, j, value);
	}

	protected void set(byte[] key, long index, T value) {
		Jedis jedis = this.pool.getResource();
		try {
			byte[] field = Long.toString(index).getBytes();

			if (value != null) {
				byte[] serializedValue = this.serializer.serialize(value);
				jedis.hset(key, field, serializedValue);
			} else {
				jedis.hdel(key, field);
			}

		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public SparseVector<T> getColumn(long i) {
		Jedis jedis = this.pool.getResource();
		try {

			byte[] columnKey = this.getColumnKey(i);
			Map<byte[], byte[]> serializedResults = jedis.hgetAll(columnKey);

			SparseVector<T> column = new RedisSparseVector<T>(serializedResults, serializer);
			return column;
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public SparseVector<T> getRow(long j) {
		Jedis jedis = this.pool.getResource();
		try {
			byte[] rowKey = this.getRowKey(j);
			Map<byte[], byte[]> serializedResults = jedis.hgetAll(rowKey);

			SparseVector<T> row = new RedisSparseVector<T>(serializedResults, serializer);
			return row;
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected byte[] getColumnKey(long i) {
		List keys = Arrays.asList(columnKey, Long.toString(i));
		return this.generateKey(keys);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected byte[] getRowKey(long j) {
		List keys = Arrays.asList(rowKey, Long.toString(j));
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

		private Map<byte[], byte[]> values = new HashMap<byte[], byte[]>();
		protected Serializer<T> serializer;

		public RedisSparseVector() {
		}

		public RedisSparseVector(Map<byte[], byte[]> values, Serializer<T> serializer) {
			this.values = values;
			this.serializer = serializer;
		}

		@Override
		public T get(long i) {
			byte[] serializedValue = this.values.get(Long.toString(i).getBytes());
			if (serializedValue == null) {
				return null;
			} else {
				return this.serializer.deserialize(serializedValue);
			}
		}

		@Override
		public void set(long i, T value) {
			byte[] key = Long.toString(i).getBytes();

			if (value != null) {
				byte[] serializedValue = this.serializer.serialize(value);
				this.values.put(key, serializedValue);
			} else {
				this.values.remove(key);
			}
		}

		@Override
		public Set<Long> indexes() {
			return Sets.newTreeSet(Iterables.transform(this.values.keySet(), new Function<byte[], Long>() {
				@Override
				public Long apply(byte[] bytes) {
					return Long.parseLong(new String(bytes));
				}

			}));
		}

		@Override
		public int size() {
			return this.values.size();
		}
	}

}
