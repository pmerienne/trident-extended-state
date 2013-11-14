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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.state.ExtendedStateFactory;
import com.github.pmerienne.trident.state.MapState;
import com.github.pmerienne.trident.state.serializer.SerializerFactory;

public class RedisMapState<K, V> extends AbstractRedisState<V> implements MapState<K, V> {

	private final Serializer<K> keySerializer;

	public RedisMapState(String id) {
		super(id);
		this.keySerializer = SerializerFactory.<K> createSerializer(config.getSerializerType());
	}

	public RedisMapState(String id, Map<String, Object> stormConfiguration) {
		super(id, stormConfiguration);
		this.keySerializer = SerializerFactory.<K> createSerializer(config.getSerializerType(), stormConfiguration);
	}

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
	}

	@Override
	public List<V> multiGet(List<K> keys) {
		List<V> results = new ArrayList<V>(keys.size());

		Jedis jedis = this.pool.getResource();
		try {
			byte[] rawKey = this.generateKey();
			byte[][] rawFields = new byte[keys.size()][];
			for (int i = 0; i < keys.size(); i++) {
				rawFields[i] = keySerializer.serialize(keys.get(i));
			}

			List<byte[]> rawResults = jedis.hmget(rawKey, rawFields);

			for (byte[] result : rawResults) {
				if (result == null) {
					results.add(null);
				} else {
					results.add(this.serializer.deserialize(result));
				}
			}
		} finally {
			this.pool.returnResource(jedis);
		}

		return results;
	}

	@Override
	public void multiPut(Map<K, V> values) {
		Jedis jedis = this.pool.getResource();
		try {
			byte[] rawKey = this.generateKey();

			Map<byte[], byte[]> rawHash = new HashMap<byte[], byte[]>(values.size());
			for (Entry<K, V> entry : values.entrySet()) {
				rawHash.put(keySerializer.serialize(entry.getKey()), serializer.serialize(entry.getValue()));
			}

			jedis.hmset(rawKey, rawHash);
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	public static class Factory<K, V> implements ExtendedStateFactory<RedisMapState<K, V>> {

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
			State state = new RedisMapState(this.id, conf);
			return state;
		}
	}
}
