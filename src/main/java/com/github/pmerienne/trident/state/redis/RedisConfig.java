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

import redis.clients.jedis.Protocol;
import com.github.pmerienne.trident.state.serializer.SerializerFactory.SerializerType;

public class RedisConfig extends HashMap<String, Object> {

	private static final long serialVersionUID = -1913847520446822065L;

	public static final String STORM_CONFIG_KEY = "redis.state.config";

	public static final String HOST = "redis.port";
	public static final String PORT = "redis.host";
	public static final String PASSWORD = "redis.password";
	public static final String KEY_PREFIX = "redis.key.prefix";
	public static final String KEY_SEPARATOR = "redis.key.separator";
	public static final String TIMEOUT = "redis.timeout";
	public static final String DATABASE = "redis.database";
	public static final String SERIALIZER_TYPE = "redis.serializer";
	public static final String MAX_ACTIVE_CONNECTION = "redis.max.active.connection";

	public final static String DEFAULT_HOST = "localhost";
	public final static String DEFAULT_PASSWORD = null;
	public final static String DEFAULT_KEY_PREFIX = "state";
	public final static String DEFAULT_KEY_SEPARATOR = ":";
	public final static SerializerType DEFAULT_SERIALIZER = SerializerType.BINARY;
	private final static int DEFAULT_MAX_ACTIVE_CONNECTION = 100;

	public RedisConfig() {
	}

	public String getHost() {
		return get(HOST, DEFAULT_HOST);
	}

	public void setHost(String host) {
		put(HOST, host);
	}

	public Integer getPort() {
		return get(PORT, Protocol.DEFAULT_PORT);
	}

	public void setPort(Integer port) {
		put(PORT, port);
	}

	public String getPassword() {
		return get(PASSWORD, DEFAULT_PASSWORD);
	}

	public void setPassword(String password) {
		put(PASSWORD, password);
	}

	public String getKeyPrefix() {
		return get(KEY_PREFIX, DEFAULT_KEY_PREFIX);
	}

	public void setKeyPrefix(String keyPrefix) {
		put(KEY_PREFIX, keyPrefix);
	}

	public String getKeySeparator() {
		return get(KEY_SEPARATOR, DEFAULT_KEY_SEPARATOR);
	}

	public void setKeySeparator(String keySeparator) {
		put(KEY_SEPARATOR, keySeparator);
	}

	public Integer getTimeout() {
		return get(TIMEOUT, Protocol.DEFAULT_TIMEOUT);
	}

	public void setTimeout(Integer timeout) {
		put(TIMEOUT, timeout);
	}

	public Integer getDatabase() {
		return get(DATABASE, Protocol.DEFAULT_DATABASE);
	}

	public void setDatabase(Integer database) {
		put(DATABASE, database);
	}

	public Integer getMaxActiveConnection() {
		return get(MAX_ACTIVE_CONNECTION, DEFAULT_MAX_ACTIVE_CONNECTION);
	}

	public void setMaxActiveConnection(Integer maxActiveConnection) {
		put(MAX_ACTIVE_CONNECTION, maxActiveConnection);
	}

	public SerializerType getSerializerType() {
		return get(SERIALIZER_TYPE, DEFAULT_SERIALIZER);
	}

	public void setSerializer(SerializerType serializerType) {
		put(SERIALIZER_TYPE, serializerType);
	}

	@SuppressWarnings("unchecked")
	private <T> T get(String key, T defaultValue) {
		return containsKey(key) ? ((T) get(key)) : defaultValue;
	}
	
	@SuppressWarnings({ "rawtypes" })
	public static RedisConfig getFromStormConfig(Map conf) {
		RedisConfig redisConfig;
		if(conf.containsKey(RedisConfig.STORM_CONFIG_KEY)) {
			redisConfig = (RedisConfig) conf.get(RedisConfig.STORM_CONFIG_KEY);
		} else {
			redisConfig = new RedisConfig();
		}
		
		return redisConfig;
	}
}
