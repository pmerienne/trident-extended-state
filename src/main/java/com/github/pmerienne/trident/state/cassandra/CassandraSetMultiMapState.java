package com.github.pmerienne.trident.state.cassandra;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import storm.trident.state.Serializer;
import storm.trident.state.State;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.state.ExtendedStateFactory;
import com.github.pmerienne.trident.state.SetMultiMapState;
import com.github.pmerienne.trident.state.cassandra.dao.SetMultiMapQueryTemplate;
import com.github.pmerienne.trident.state.serializer.SerializerFactory;

public class CassandraSetMultiMapState<K, V> extends AbstractCassandraState<V> implements SetMultiMapState<K, V> {

	private final SetMultiMapQueryTemplate template;

	public CassandraSetMultiMapState(String id, Serializer<V> serializer, SetMultiMapQueryTemplate template) {
		super(id, serializer);
		this.template = template;
	}

	@Override
	public void beginCommit(Long txid) {
		// TODO Auto-generated method stub

	}

	@Override
	public void commit(Long txid) {
		// TODO Auto-generated method stub

	}

	@Override
	public long sizeOf(K key) {
		return template.sizeOf(id, key);
	}

	@Override
	public void put(K key, V value) {
		template.put(id, key, value, serializer);
	}

	@Override
	public Set<V> get(K key) {
		return template.get(id, key, serializer);
	}

	public static class Factory<K, V> implements ExtendedStateFactory<CassandraSetMultiMapState<K, V>> {
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
			final Serializer<V> serializer = SerializerFactory
					.<V> createSerializer(SerializerFactory.SerializerType.HUMAN_READABLE);
			State state = new CassandraSetMultiMapState<K, V>(this.id, serializer, config.getDao()
					.getSetMultiMapQueryTemplate());
			return state;
		}
	}
}
