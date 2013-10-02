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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.state.SparseMatrixState;

public class RedisSparseMatrixState<T> extends AbstractRedisState implements SparseMatrixState<T> {

	private final static String COLUMN_KEY = "column";
	private final static String ROW_KEY = "row";

	public RedisSparseMatrixState(String id) {
		super(id);
	}

	public RedisSparseMatrixState(String id, String host, int port) {
		super(id, host, port);
	}

	@Override
	@SuppressWarnings("unchecked")
	public T get(long i, long j) {
		Jedis jedis = this.pool.getResource();

		String rowKey = this.getRowKey(j);
		Set<String> values = jedis.zrangeByScore(rowKey, (double) i, (double) i);

		if (values == null || values.isEmpty()) {
			return null;
		} else {
			String serializedValue = values.iterator().next();
			ValueWrapper<T> wrappedValue = (ValueWrapper<T>) this.serializer.deserialize(serializedValue.getBytes());
			return wrappedValue.value;
		}
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

		// Remove previous value
		jedis.zremrangeByScore(key, (double) index, (double) index);

		if (value != null) {
			// Add new value
			byte[] serializedValue = this.serializer.serialize(new ValueWrapper<T>(index, value));
			jedis.zadd(key, (double) index, new String(serializedValue));
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public SparseVector<T> getColumn(long i) {
		Jedis jedis = this.pool.getResource();

		String columnKey = this.getColumnKey(i);
		Set<Tuple> indexedValues = jedis.zrangeByScoreWithScores(columnKey, "0", "+inf");

		SparseVector<T> column = new SparseVector<T>();
		if (indexedValues != null) {
			for (Tuple indexedValue : indexedValues) {
				ValueWrapper<T> wrappedValue = (ValueWrapper<T>) this.serializer.deserialize(indexedValue.getBinaryElement());
				column.set((long) indexedValue.getScore(), wrappedValue.value);
			}

		}

		return column;
	}

	@Override
	public void setColumn(long i, SparseVector<T> column) {
		Jedis jedis = this.pool.getResource();
		String columnKey = this.getColumnKey(i);

		jedis.del(columnKey);

		Map<Double, String> scoreMembers = new HashMap<Double, String>();
		ValueWrapper<T> wrappedValue;
		for (long index : column.indexes()) {
			wrappedValue = new ValueWrapper<T>(index, column.get(index));
			scoreMembers.put((double) index, new String(this.serializer.serialize(wrappedValue)));
		}

		jedis.zadd(columnKey, scoreMembers);
	}

	@Override
	@SuppressWarnings("unchecked")
	public SparseVector<T> getRow(long j) {
		Jedis jedis = this.pool.getResource();

		String rowKey = this.getRowKey(j);
		Set<Tuple> indexedValues = jedis.zrangeByScoreWithScores(rowKey, "0", "+inf");

		SparseVector<T> row = new SparseVector<T>();
		if (indexedValues != null) {
			for (Tuple indexedValue : indexedValues) {
				ValueWrapper<T> wrappedValue = (ValueWrapper<T>) this.serializer.deserialize(indexedValue.getBinaryElement());
				row.set((long) indexedValue.getScore(), wrappedValue.value);
			}

		}

		return row;
	}

	@Override
	public void setRow(long j, SparseVector<T> row) {
		Jedis jedis = this.pool.getResource();
		String rowKey = this.getRowKey(j);

		jedis.del(rowKey);

		Map<Double, String> scoreMembers = new HashMap<Double, String>();
		ValueWrapper<T> wrappedValue;
		for (long index : row.indexes()) {
			wrappedValue = new ValueWrapper<T>(index, row.get(index));
			scoreMembers.put((double) index, new String(this.serializer.serialize(wrappedValue)));
		}

		jedis.zadd(rowKey, scoreMembers);
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

	public static class Factory implements StateFactory {

		private static final long serialVersionUID = 4718043951532492603L;

		private String id;

		public Factory(String id) {
			this.id = id;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			State state;
			String host = getHost(conf);
			Integer port = getPort(conf);

			if (host != null && port != null) {
				state = new RedisSparseMatrixState(this.id, host, port);
			} else {
				state = new RedisSparseMatrixState(this.id);
			}

			return state;
		}
	}

	private static class ValueWrapper<T> {

		public long index;
		public T value;

		@SuppressWarnings("unused")
		public ValueWrapper() {
		}

		public ValueWrapper(long index, T value) {
			this.index = index;
			this.value = value;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int) (index ^ (index >>> 32));
			result = prime * result + ((value == null) ? 0 : value.hashCode());
			return result;
		}

		@SuppressWarnings("unchecked")
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ValueWrapper<T> other = (ValueWrapper<T>) obj;
			if (index != other.index)
				return false;
			if (value == null) {
				if (other.value != null)
					return false;
			} else if (!value.equals(other.value))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "ValueWrapper [index=" + index + ", value=" + value + "]";
		}

	}
}
