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
package com.github.pmerienne.trident.state.serializer;

import storm.trident.state.Serializer;
import flexjson.JSONDeserializer;
import flexjson.JSONSerializer;

public class JsonValueSerializer<T> implements Serializer<T> {

	private static final long serialVersionUID = -2910589120089169978L;

	private JSONSerializer serializer = new JSONSerializer();
	private JSONDeserializer<T> deserializer = new JSONDeserializer<T>();

	@Override
	public byte[] serialize(T obj) {
		String json = this.serializer.deepSerialize(obj);
		return json.getBytes();
	}

	@Override
	public T deserialize(byte[] bytes) {
		String json = new String(bytes);
		return this.deserializer.deserialize(json);
	}
}
