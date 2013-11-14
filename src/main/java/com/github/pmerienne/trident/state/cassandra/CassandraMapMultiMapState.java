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
package com.github.pmerienne.trident.state.cassandra;

import java.util.Map;
import java.util.UUID;

import storm.trident.state.Serializer;
import storm.trident.state.State;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.state.ExtendedStateFactory;
import com.github.pmerienne.trident.state.MapMultimapState;
import com.github.pmerienne.trident.state.cassandra.dao.MapMultiMapQueryTemplate;
import com.github.pmerienne.trident.state.serializer.SerializerFactory;

public class CassandraMapMultiMapState<K1, K2, V> extends AbstractCassandraState<V> implements
		MapMultimapState<K1, K2, V> {

	private final MapMultiMapQueryTemplate template;
	private final Serializer<K2> subKeySerializer;

	public CassandraMapMultiMapState(String id, Serializer<K2> subKeySerializer, Serializer<V> serializer,
			MapMultiMapQueryTemplate template) {
		super(id, serializer);
		this.subKeySerializer = subKeySerializer;
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
	public void put(K1 key, K2 subkey, V value) {
		template.put(id, key, subkey, value, subKeySerializer, serializer);
	}

	@Override
	public V get(K1 key, K2 subkey) {
		return template.get(id, key, subkey, subKeySerializer, serializer);
	}

	@Override
	public Map<K2, V> getAll(K1 key) {
		return template.getAll(id, key, subKeySerializer, serializer);
	}

	public static class Factory<K1, K2, V> implements ExtendedStateFactory<CassandraMapMultiMapState<K1, K2, V>> {
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
			final Serializer<K2> subKeySerializer = SerializerFactory
					.<K2> createSerializer(SerializerFactory.SerializerType.BINARY);
			final Serializer<V> serializer = SerializerFactory
					.<V> createSerializer(SerializerFactory.SerializerType.HUMAN_READABLE);
			State state = new CassandraMapMultiMapState<K1, K2, V>(this.id, subKeySerializer, serializer, config
					.getDao().getMapMultiMapQueryTemplate());
			return state;
		}
	}
}
