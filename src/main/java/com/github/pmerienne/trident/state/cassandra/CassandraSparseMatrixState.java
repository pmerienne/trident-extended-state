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
import com.github.pmerienne.trident.state.SparseMatrixState;
import com.github.pmerienne.trident.state.cassandra.dao.SparseMatrixQueryTemplate;
import com.github.pmerienne.trident.state.serializer.SerializerFactory;

public class CassandraSparseMatrixState<V> extends AbstractCassandraState<V> implements SparseMatrixState<V> {

	private final SparseMatrixQueryTemplate template;

	public CassandraSparseMatrixState(String id, Serializer<V> serializer, SparseMatrixQueryTemplate template) {
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
	public V get(long i, long j) {
		return template.get(id, i, j, serializer);
	}

	@Override
	public void set(long i, long j, V value) {
		template.set(id, i, j, value, serializer);
	}

	@Override
	public SparseVector<V> getColumn(long i) {
		return template.getColumnsFromRow(id, i, serializer);
	}

	@Override
	public SparseVector<V> getRow(long j) {
		return template.getRowsFromColumn(id, j, serializer);
	}

	public static class Factory<T> implements ExtendedStateFactory<CassandraSparseMatrixState<T>> {
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
			State state = new CassandraSparseMatrixState<T>(this.id, serializer, config.getDao()
					.getSparseMatrixQueryTemplate());
			return state;
		}
	}
}
