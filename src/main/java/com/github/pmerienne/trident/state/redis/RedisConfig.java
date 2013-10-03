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

import com.github.pmerienne.trident.state.serializer.JsonValueSerializer;

import redis.clients.jedis.Protocol;
import storm.trident.state.Serializer;

public class RedisConfig extends HashMap<String, String> {

	private static final long serialVersionUID = -1913847520446822065L;

	public static final String HOST = "redis.port";
	public static final String PORT = "redis.host";
	public static final String PASSWORD = "redis.password";
	public static final String KEY_PREFIX = "redis.key.prefix";
	public static final String KEY_SEPARATOR = "redis.key.separator";
	public static final String TIMEOUT = "redis.timeout";
	public static final String DATABASE = "redis.database";
	public static final String SERIALIZER = "redis.serializer";

	public final static String DEFAULT_HOST = "localhost";
	public final static String DEFAULT_PASSWORD = null;
	public final static String DEFAULT_KEY_PREFIX = "state";
	public final static String DEFAULT_KEY_SEPARATOR = ":";
	public final static String DEFAULT_SERIALIZER = JsonValueSerializer.class.getName();

	public RedisConfig() {
	}

	public RedisConfig(Map<?, ?> conf) {
		super();

		Object value;
		for (Object key : conf.keySet()) {
			value = conf.get(key);
			put(key instanceof String ? (String) key : key.toString(), value instanceof String ? (String) value : value.toString());
		}
	}

	public String getHost() {
		return getString(HOST, DEFAULT_HOST);
	}

	public void setHost(String host) {
		put(HOST, host);
	}

	public Integer getPort() {
		return getInteger(PORT, Protocol.DEFAULT_PORT);
	}

	public void setPort(Integer port) {
		put(PORT, Integer.toString(port));
	}

	public String getPassword() {
		return getString(PASSWORD, DEFAULT_PASSWORD);
	}

	public void setPassword(String password) {
		put(PASSWORD, password);
	}

	public String getKeyPrefix() {
		return getString(KEY_PREFIX, DEFAULT_KEY_PREFIX);
	}

	public void setKeyPrefix(String keyPrefix) {
		put(KEY_PREFIX, keyPrefix);
	}

	public String getKeySeparator() {
		return getString(KEY_SEPARATOR, DEFAULT_KEY_SEPARATOR);
	}

	public void setKeySeparator(String keySeparator) {
		put(KEY_SEPARATOR, keySeparator);
	}

	public Integer getTimeout() {
		return getInteger(TIMEOUT, Protocol.DEFAULT_TIMEOUT);
	}

	public void setTimeout(Integer timeout) {
		put(TIMEOUT, Integer.toString(timeout));
	}

	public Integer getDatabase() {
		return getInteger(DATABASE, Protocol.DEFAULT_DATABASE);
	}

	public void setDatabase(Integer database) {
		put(DATABASE, Integer.toString(database));
	}

	@SuppressWarnings("unchecked")
	public <T> Serializer<T> getSerializer() {
		String serializerClass = getString(SERIALIZER, DEFAULT_SERIALIZER);
		try {
			Class<Serializer<T>> clazz = (Class<Serializer<T>>) Class.forName(serializerClass);
			return clazz.newInstance();
		} catch (Exception e) {
			// TODO : log
			return null;
		}
	}

	public void setSerializer(Class<Serializer<?>> serializerClass) {
		put(SERIALIZER, serializerClass.getName());
	}

	private String getString(String key, String defaultValue) {
		return containsKey(key) ? get(key) : defaultValue;
	}

	private Integer getInteger(String key, Integer defaultValue) {
		return containsKey(key) ? Integer.parseInt(get(key)) : defaultValue;
	}
}
