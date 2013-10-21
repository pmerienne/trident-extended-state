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

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import storm.trident.state.Serializer;

import com.github.pmerienne.trident.state.ExtendedState;

public abstract class AbstractRedisState<T> implements ExtendedState<T> {

	protected final String id;
	protected final RedisConfig config;

	protected final JedisPool pool;

	protected final Serializer<T> serializer;

	public AbstractRedisState(String id, RedisConfig config) {
		this.id = id;
		this.config = config;
		this.serializer = config.<T> getSerializer();

		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		jedisPoolConfig.setMaxActive(config.getMaxActiveConnection());
		this.pool = new JedisPool(jedisPoolConfig, config.getHost(), config.getPort(), config.getTimeout(), null, config.getDatabase());
	}

	public AbstractRedisState(String id) {
		this(id, new RedisConfig());
	}

	protected String generateKey() {
		StringBuilder sb = new StringBuilder(config.getKeyPrefix()).append(config.getKeySeparator()).append(this.id);
		return sb.toString();
	}

	protected String generateKey(Object key) {
		StringBuilder sb = new StringBuilder(config.getKeyPrefix()).append(config.getKeySeparator()).append(this.id).append(config.getKeySeparator()).append(key.toString());
		return sb.toString();
	}

	protected String generateKey(List<Object> keys) {
		StringBuilder sb = new StringBuilder(config.getKeyPrefix()).append(config.getKeySeparator()).append(this.id);
		for (Object key : keys) {
			sb.append(config.getKeySeparator()).append(key.toString());
		}
		return sb.toString();
	}

	public void flushAll() {
		Jedis jedis = this.pool.getResource();
		try {
			jedis.flushDB();
		} finally {
			this.pool.returnResource(jedis);
		}
	}

}
