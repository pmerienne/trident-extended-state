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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import storm.trident.state.State;
import backtype.storm.task.IMetricsContext;

import com.github.pmerienne.trident.state.ExtendedStateFactory;
import com.github.pmerienne.trident.state.MapState;

public class MemoryMapState<K, V> implements MapState<K, V> {

	@SuppressWarnings("rawtypes")
	private static ConcurrentHashMap<String, Map> DBS = new ConcurrentHashMap<String, Map>();

	private final Map<K, V> db;

	public MemoryMapState(String id) {
		this.db = getDB(id);
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
	public List<V> multiGet(List<K> keys) {
		List<V> results = new ArrayList<V>(keys.size());

		for (K key : keys) {
			results.add(db.get(key));
		}

		return results;
	}

	@Override
	public void multiPut(Map<K, V> values) {
		db.putAll(values);
	}

	protected static void clear() {
		DBS.clear();
	}

	@SuppressWarnings("unchecked")
	public static <K, V> Map<K, V> getDB(String id) {
		Map<K, V> map;

		if (DBS.contains(id)) {
			map = DBS.get(id);
		} else {
			map = new HashMap<K, V>();
			DBS.put(id, map);
		}

		return map;
	}

	@SuppressWarnings({ "rawtypes" })
	public static class Factory<K, V> implements ExtendedStateFactory<MemoryMapState<K, V>> {

		private static final long serialVersionUID = 1375910273264827533L;

		private final String id;

		public Factory() {
			this.id = UUID.randomUUID().toString();
		}

		public Factory(String id) {
			this.id = id;
		}

		@Override
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new MemoryMapState(this.id);
		}
	}
}
