package com.github.pmerienne.trident.state.cassandra;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.ValueUpdater;
import storm.trident.state.map.MapState;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.state.ExtendedStateFactory;
import com.github.pmerienne.trident.state.cassandra.dao.MapQueryTemplate;
import com.github.pmerienne.trident.state.serializer.SerializerFactory;
import com.github.pmerienne.trident.state.serializer.SerializerFactory.SerializerType;

public class CassandraMapState<T> extends AbstractCassandraState<T> implements MapState<T> {

	private final MapQueryTemplate template;
	private final Serializer<Object> jsonSerializer;

	public CassandraMapState(String id, Serializer<T> serializer, Serializer<Object> jsonSerializer,
			MapQueryTemplate template) {
		super(id, serializer);
		this.jsonSerializer = jsonSerializer;
		this.template = template;
	}

	@Override
	public List<T> multiGet(List<List<Object>> keys) {
		List<String> keysString = buildKeysString(keys);
		return template.multiGet(id, keysString, serializer);
	}

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
		// TODO Auto-generated method stub

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
		List<T> curr = this.multiGet(keys);
		List<T> ret = new ArrayList<T>(curr.size());
		for (int i = 0; i < curr.size(); i++) {
			T currVal = curr.get(i);
			ValueUpdater<T> updater = updaters.get(i);
			ret.add(updater.update(currVal));
		}
		this.multiPut(keys, ret);
		return ret;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<T> vals) {
		List<String> keysString = buildKeysString(keys);
		template.multiPut(id, keysString, vals, serializer);
	}

	private List<String> buildKeysString(List<List<Object>> keys) {
		List<String> keysString = new ArrayList<>();
		for (List<Object> subKeys : keys) {
			List<String> subKeysString = new ArrayList<>();
			for (Object subKey : subKeys) {
				subKeysString.add(new String(jsonSerializer.serialize(subKey)));
			}
			keysString.add(StringUtils.join(subKeysString, ":"));
		}
		return keysString;
	}

	public static class Factory<T> implements ExtendedStateFactory<CassandraMapState<T>> {
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
			final Serializer<Object> jsonSerializer = SerializerFactory
					.<Object> createSerializer(SerializerType.HUMAN_READABLE);
			final Serializer<T> serializer = SerializerFactory
					.<T> createSerializer(SerializerFactory.SerializerType.HUMAN_READABLE);
			State state = new CassandraMapState<T>(this.id, serializer, jsonSerializer, config.getDao()
					.getMapQueryTemplate());
			return state;
		}
	}
}
