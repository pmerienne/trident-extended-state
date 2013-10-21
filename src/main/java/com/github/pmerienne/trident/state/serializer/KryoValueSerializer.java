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

import java.nio.ByteBuffer;

import storm.trident.state.Serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoValueSerializer<T> implements Serializer<T> {

	private static final long serialVersionUID = 1689053104987281146L;

	private final Kryo kryo;
	private final int capacity;

	public KryoValueSerializer() {
		this(new Kryo(), 16 * 1024);
	}

	public KryoValueSerializer(int capacity) {
		this(new Kryo(), capacity);
	}

	public KryoValueSerializer(Kryo kryo, int capacity) {
		this.kryo = kryo;
		this.capacity = capacity;
	}

	@Override
	public byte[] serialize(T obj) {
		byte[] buffer = ByteBuffer.allocate(capacity).array();
		Output output = new Output(buffer);

		this.kryo.writeClassAndObject(output, obj);
		output.close();

		return buffer;
	}

	@Override
	@SuppressWarnings("unchecked")
	public T deserialize(byte[] bytes) {
		Input input = new Input(bytes);
		Object obj = this.kryo.readClassAndObject(input);
		return (T) obj;
	}
}