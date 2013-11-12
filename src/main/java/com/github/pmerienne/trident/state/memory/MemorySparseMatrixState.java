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
package com.github.pmerienne.trident.state.memory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import storm.trident.state.State;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.state.ExtendedStateFactory;
import com.github.pmerienne.trident.state.SparseMatrixState;
import com.github.pmerienne.trident.state.util.MapStateUtil;

public class MemorySparseMatrixState<T> extends AbstractMemoryState<Map<Long, T>> implements SparseMatrixState<T> {

	private final static String COLUMN_KEY = "column:";
	private final static String ROW_KEY = "row:";

	public MemorySparseMatrixState(String id) {
		super(id);
	}

	@Override
	public T get(long i, long j) {
		Map<Long, T> column = MapStateUtil.getSingle(this, COLUMN_KEY + i);
		return column == null ? null : column.get(j);
	}

	@Override
	public void set(long i, long j, T value) {
		Map<Long, T> column = MapStateUtil.getSingle(this, COLUMN_KEY + i);
		column = column == null ? new HashMap<Long, T>() : new HashMap<Long, T>(column);
		column.put(j, value);
		MapStateUtil.putSingle(this, COLUMN_KEY + i, column);

		Map<Long, T> row = MapStateUtil.getSingle(this, ROW_KEY + j);
		row = row == null ? new HashMap<Long, T>() : new HashMap<Long, T>(row);
		row.put(i, value);
		MapStateUtil.putSingle(this, ROW_KEY + j, row);
	}

	@Override
	public SparseVector<T> getColumn(long i) {
		Map<Long, T> column = MapStateUtil.getSingle(this, COLUMN_KEY + i);
		return new SparseVector<T>(column);
	}

	@Override
	public SparseVector<T> getRow(long j) {
		Map<Long, T> row = MapStateUtil.getSingle(this, ROW_KEY + j);
		return new SparseVector<T>(row);
	}

	public static class Factory<T> implements ExtendedStateFactory<MemorySparseMatrixState<T>> {

		private static final long serialVersionUID = 4957447552599428092L;

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
			State state = new MemorySparseMatrixState(this.id);
			return state;
		}
	}
}
