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
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.state.SetState;

public class MemorySetState<T> extends TransactionalMemoryMapState<Set<T>> implements SetState<T> {

	public MemorySetState(String id) {
		super(id);
	}

	@Override
	public Set<T> get() {
		Set<T> set = super.get();
		if (set == null) {
			set = new HashSet<T>();
		}
		return set;
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
	public static class Factory implements StateFactory {

		private static final long serialVersionUID = -6865870100536320916L;

		private final String _id;

		public Factory() {
			this._id = UUID.randomUUID().toString();
		}

		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new MemorySetState(this._id);
		}
	}
}
