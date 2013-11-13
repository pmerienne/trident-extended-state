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
