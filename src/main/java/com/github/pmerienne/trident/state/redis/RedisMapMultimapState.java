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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.state.ExtendedStateFactory;
import com.github.pmerienne.trident.state.MapMultimapState;
import com.github.pmerienne.trident.state.serializer.SerializerFactory;

public class RedisMapMultimapState<K1, K2, V> extends AbstractRedisState<V> implements MapMultimapState<K1, K2, V> {

	private final Serializer<K2> keySerializer;

	public RedisMapMultimapState(String id) {
		super(id);
		this.keySerializer = SerializerFactory.<K2> createSerializer(config.getSerializerType());
	}

	public RedisMapMultimapState(String id, Map<String, Object> stormConfiguration) {
		super(id, stormConfiguration);
		this.keySerializer = SerializerFactory.<K2> createSerializer(config.getSerializerType(), stormConfiguration);
	}

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
	}

	@Override
	public void put(K1 key, K2 subkey, V value) {
		Jedis jedis = this.pool.getResource();
		try {
			byte[] rawKey = this.generateKey(key);
			byte[] rawSubKey = this.keySerializer.serialize(subkey);
			jedis.hset(rawKey, rawSubKey, this.serializer.serialize(value));
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public V get(K1 key, K2 subkey) {
		Jedis jedis = this.pool.getResource();
		V result = null;
		try {
			byte[] rawKey = this.generateKey(key);
			byte[] rawSubKey = this.keySerializer.serialize(subkey);
			byte[] resultAsString = jedis.hget(rawKey, rawSubKey);
			if (resultAsString != null) {
				result = this.serializer.deserialize(resultAsString);
			}
		} finally {
			this.pool.returnResource(jedis);
		}

		return result;
	}

	@Override
	public Map<K2, V> getAll(K1 key) {
		Map<K2, V> results = new HashMap<K2, V>();

		Jedis jedis = this.pool.getResource();
		try {
			byte[] rawKey = this.generateKey(key);
			Map<byte[], byte[]> rawResults = jedis.hgetAll(rawKey);

			K2 subkey;
			V value;
			for (byte[] subKey : rawResults.keySet()) {
				subkey = this.keySerializer.deserialize(subKey);
				value = this.serializer.deserialize(rawResults.get(subKey));
				results.put(subkey, value);
			}

		} finally {
			this.pool.returnResource(jedis);
		}

		return results;
	}

	public static class Factory<K1, K2, V> implements ExtendedStateFactory<RedisMapMultimapState<K1, K2, V>> {

		private static final long serialVersionUID = 4718043951532492603L;

		private final String id;

		public Factory() {
			this.id = UUID.randomUUID().toString();
		}

		public Factory(String id) {
			this.id = id;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			State state = new RedisMapMultimapState(this.id, conf);
			return state;
		}
	}
}
