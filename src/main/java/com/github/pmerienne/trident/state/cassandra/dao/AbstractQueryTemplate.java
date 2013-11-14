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
package com.github.pmerienne.trident.state.cassandra.dao;

import java.nio.ByteBuffer;

import storm.trident.state.Serializer;

public class AbstractQueryTemplate {

	protected <E> ByteBuffer serialize(E e, Serializer<E> serializer) {
		byte[] serialized = serializer.serialize(e);

		ByteBuffer bb = null;
		if (serialized.length > 0) {
			bb = ByteBuffer.wrap(serialized);
		}
		return bb;
	}

	protected <E> E deserialize(ByteBuffer bb, Serializer<E> serializer) {
		E value = null;
		byte[] bytes = new byte[bb.remaining()];
		bb.get(bytes);

		if (bytes.length > 0) {
			value = serializer.deserialize(bytes);
		}
		return value;
	}
}
