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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import storm.trident.state.State;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.state.ExtendedStateFactory;
import com.github.pmerienne.trident.state.SetState;

public class RedisSetState<T> extends AbstractRedisState<T> implements SetState<T> {

	public RedisSetState(String id) {
		super(id);
	}

	public RedisSetState(String id, Map<String, Object> stormConfiguration) {
		super(id, stormConfiguration);
	}

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
	}

	@Override
	public void add(T e) {
		Jedis jedis = this.pool.getResource();
		try {
			byte[] key = this.generateKey();
			jedis.sadd(key, this.serializer.serialize(e));
		} finally {
			this.pool.returnResource(jedis);
		}
	}

	@Override
	public void addAll(Collection<? extends T> c) {
		Jedis jedis = this.pool.getResource();

		byte[] key = this.generateKey();
		byte[][] members = new byte[c.size()][];
		int i = 0;
		for (T element : c) {
			members[i] = this.serializer.serialize(element);
			i++;
		}

		try {
			jedis.sadd(key, members);
		} finally {
			this.pool.returnResource(jedis);
		}

	}

	@Override
	public Set<T> get() {
		Set<T> results = new HashSet<T>();

		Jedis jedis = this.pool.getResource();
		try {
			byte[] key = this.generateKey();
			Set<byte[]> rawResults = jedis.smembers(key);
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
	public void clear() {
		Jedis jedis = this.pool.getResource();
		try {
			byte[] key = this.generateKey();
			jedis.del(key);
		} finally {
			this.pool.returnResource(jedis);
		}

	}

	public static class Factory<T> implements ExtendedStateFactory<RedisSetState<T>> {

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
			State state = new RedisSetState(this.id, conf);
			return state;
		}
	}

}
