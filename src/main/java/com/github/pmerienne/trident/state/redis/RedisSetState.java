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

	public RedisSetState(String id, RedisConfig config) {
		super(id, config);
	}

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
	}

	@Override
	public boolean add(T e) {
		Jedis jedis = this.pool.getResource();
		long result;
		try {
			String key = this.generateKey();
			result = jedis.sadd(key, new String(this.serializer.serialize(e)));
		} finally {
			this.pool.returnResource(jedis);
		}

		return result >= 1;
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		Jedis jedis = this.pool.getResource();
		long result;

		String key = this.generateKey();
		String[] members = new String[c.size()];
		int i = 0;
		for (T element : c) {
			members[i] = new String(this.serializer.serialize(element));
			i++;
		}

		try {
			result = jedis.sadd(key, members);
		} finally {
			this.pool.returnResource(jedis);
		}

		return result >= 1;
	}

	@Override
	public Set<T> get() {
		Set<T> results = new HashSet<T>();

		Jedis jedis = this.pool.getResource();
		try {
			String key = this.generateKey();
			Set<String> resultsAsString = jedis.smembers(key);
			for (String result : resultsAsString) {
				if (result == null || result.isEmpty()) {
					results.add(null);
				} else {
					results.add(this.serializer.deserialize(result.getBytes()));
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
			String key = this.generateKey();
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

		@SuppressWarnings("rawtypes")
		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			State state = new RedisSetState(this.id, new RedisConfig(conf));
			return state;
		}
	}

}
