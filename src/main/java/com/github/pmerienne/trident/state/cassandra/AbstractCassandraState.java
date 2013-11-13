package com.github.pmerienne.trident.state.cassandra;

import storm.trident.state.Serializer;

import com.github.pmerienne.trident.state.ExtendedState;

public abstract class AbstractCassandraState<T> implements ExtendedState<T> {

	protected String id;
	protected Serializer<T> serializer;

	protected AbstractCassandraState(String id, Serializer<T> serializer) {
		this.id = id;
		this.serializer = serializer;
	}

}
