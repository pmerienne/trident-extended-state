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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import storm.trident.state.State;
import storm.trident.state.ValueUpdater;
import storm.trident.state.map.MapState;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.state.ExtendedStateFactory;

public class RedisMapState<T> extends AbstractRedisState<T> implements MapState<T> {

	public RedisMapState(String id) {
		super(id);
	}

	public RedisMapState(String id, Map<String, Object> stormConfiguration) {
		super(id, stormConfiguration);
	}

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
	}

	@Override
	public List<T> multiGet(List<List<Object>> keys) {
		List<T> results = new ArrayList<T>();

		// Create redis String keys
		byte[][] stringKeys = new byte[keys.size()][];
		for (int i = 0; i < keys.size(); i++) {
			stringKeys[i] = this.generateKey(keys.get(i));
		}

		// Call redis server
		Jedis jedis = this.pool.getResource();
		try {
			List<byte[]> rawResults = jedis.mget(stringKeys);
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
	public void multiPut(List<List<Object>> keys, List<T> vals) {
		byte[][] keyValues = new byte[keys.size() * 2][];
		for (int i = 0; i < keys.size(); i++) {
			keyValues[i * 2] = this.generateKey(keys.get(i));
			keyValues[i * 2 + 1] = this.serializer.serialize(vals.get(i));
		}

		// Call redis server
		Jedis jedis = this.pool.getResource();
		try {
			jedis.mset(keyValues);
		} finally {
			this.pool.returnResource(jedis);
		}

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
		List<T> curr = this.multiGet(keys);
		List<T> ret = new ArrayList<T>(curr.size());
		for (int i = 0; i < curr.size(); i++) {
			T currVal = curr.get(i);
			ValueUpdater<T> updater = updaters.get(i);
			ret.add(updater.update(currVal));
		}
		this.multiPut(keys, ret);
		return ret;
	}

	public static class Factory<T> implements ExtendedStateFactory<RedisMapState<T>> {

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
