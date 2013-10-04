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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import storm.trident.state.State;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.state.ExtendedStateFactory;
import com.github.pmerienne.trident.state.SetState;

public class MemorySetState<T> extends TransactionalMemoryMapState<Set<T>> implements SetState<T> {

	public MemorySetState(String id) {
		super(id);
	}

	@Override
	public Set<T> get() {
		Set<T> set = super.get();
		return set == null ? new HashSet<T>() : new HashSet<T>(set);
	}

	@Override
	public boolean add(T e) {
		Set<T> set = this.get();
		boolean result = set.add(e);
		this.set(set);
		return result;
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		Set<T> set = this.get();
		boolean result = set.addAll(c);
		this.set(set);
		return result;
	}

	@Override
	public void clear() {
		this.set(new HashSet<T>());
	}

	@SuppressWarnings({ "rawtypes" })
	public static class Factory<T> implements ExtendedStateFactory<MemorySetState<T>> {

		private static final long serialVersionUID = 4769786989416998195L;

		private final String _id;

		public Factory() {
			this._id = UUID.randomUUID().toString();
		}

		public Factory(String _id) {
			this._id = _id;
		}

		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new MemorySetState(this._id);
		}
	}
}
