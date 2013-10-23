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
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import storm.trident.state.Serializer;

import com.github.pmerienne.trident.state.ExtendedState;
import com.github.pmerienne.trident.state.serializer.SerializerFactory;

public abstract class AbstractRedisState<T> implements ExtendedState<T> {

	protected final String id;
	protected final RedisConfig config;

	protected final JedisPool pool;

	protected final Serializer<T> serializer;

	public AbstractRedisState(String id) {
		this.id = id;
		this.config = new RedisConfig();
		this.serializer = SerializerFactory.<T> createSerializer(config.getSerializerType());

		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		jedisPoolConfig.setMaxActive(config.getMaxActiveConnection());
		this.pool = new JedisPool(jedisPoolConfig, config.getHost(), config.getPort(), config.getTimeout(), null, config.getDatabase());
	}

	public AbstractRedisState(String id, Map<String, Object> stormConfiguration) {
		this.id = id;
		this.config = RedisConfig.getFromStormConfig(stormConfiguration);
		this.serializer = SerializerFactory.<T> createSerializer(config.getSerializerType(), stormConfiguration);

		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		jedisPoolConfig.setMaxActive(config.getMaxActiveConnection());
		this.pool = new JedisPool(jedisPoolConfig, config.getHost(), config.getPort(), config.getTimeout(), null, config.getDatabase());
	}

	protected byte[] generateKey() {
		StringBuilder sb = new StringBuilder(config.getKeyPrefix()).append(config.getKeySeparator()).append(this.id);
		return sb.toString().getBytes();
	}

	protected byte[] generateKey(Object key) {
		StringBuilder sb = new StringBuilder(config.getKeyPrefix()).append(config.getKeySeparator()).append(this.id).append(config.getKeySeparator()).append(key.toString());
		return sb.toString().getBytes();
	}

	protected byte[] generateKey(List<Object> keys) {
		StringBuilder sb = new StringBuilder(config.getKeyPrefix()).append(config.getKeySeparator()).append(this.id);
		for (Object key : keys) {
			sb.append(config.getKeySeparator()).append(key.toString());
		}
		return sb.toString().getBytes();
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
