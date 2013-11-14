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

import java.util.Map;

import storm.trident.state.Serializer;

public class SerializerFactory {

	public static <T> Serializer<T> createSerializer(SerializerType type) {
		switch (type) {
		case BINARY:
			return new KryoValueSerializer<T>();
		case HUMAN_READABLE:
			return new JsonValueSerializer<T>();
		default:
			return new KryoValueSerializer<T>();
		}
	}

	@SuppressWarnings("rawtypes")
	public static <T> Serializer<T> createSerializer(SerializerType type, Map stormconfig) {
		switch (type) {
		case BINARY:
			return new KryoValueSerializer<T>(stormconfig);
		case HUMAN_READABLE:
			return new JsonValueSerializer<T>();
		default:
			return new KryoValueSerializer<T>(stormconfig);
		}
	}

    public static enum SerializerType {
        BINARY, HUMAN_READABLE;
    }
}
