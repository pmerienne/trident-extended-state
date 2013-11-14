package com.github.pmerienne.trident.state.cassandra;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import storm.trident.state.Serializer;
import storm.trident.state.State;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.state.ExtendedStateFactory;
import com.github.pmerienne.trident.state.SetState;
import com.github.pmerienne.trident.state.cassandra.dao.SetQueryTemplate;
import com.github.pmerienne.trident.state.serializer.SerializerFactory;

public class CassandraSetState<T> extends AbstractCassandraState<T> implements SetState<T> {

	private final SetQueryTemplate template;

	public CassandraSetState(String id, Serializer<T> serializer, SetQueryTemplate template) {
		super(id, serializer);
		this.template = template;
	}

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
	}

	@Override
	public void add(T e) {
		try {
			template.addToSet(id, e, serializer);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void addAll(Collection<? extends T> c) {
		try {
			template.addAllToSet(id, c, serializer);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public Set<T> get() {
		try {
			return template.getAll(id, serializer);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void clear() {
		template.removeAll(id);
	}

	public static class Factory<T> implements ExtendedStateFactory<CassandraSetState<T>> {
		private static final long serialVersionUID = 4718043951532492603L;
		private final String id;

		public Factory() {
			this.id = UUID.randomUUID().toString();
		}

		public Factory(String id) {
			this.id = id;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {

			final CassandraConfig config = CassandraConfig.getFromStormConfig(conf);
			final Serializer<T> serializer = SerializerFactory
					.<T> createSerializer(SerializerFactory.SerializerType.HUMAN_READABLE);
			State state = new CassandraSetState<T>(this.id, serializer, config.getDao().getSetQueryTemplate());
			return state;
		}
	}
}
