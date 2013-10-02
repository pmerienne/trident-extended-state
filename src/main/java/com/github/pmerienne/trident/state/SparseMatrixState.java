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
package com.github.pmerienne.trident.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public interface SparseMatrixState<T> {

	T get(long i, long j);

	void set(long i, long j, T value);

	SparseVector<T> getColumn(long i);

	void setColumn(long i, SparseVector<T> column);

	SparseVector<T> getRow(long j);

	void setRow(long i, SparseVector<T> row);

	public static class SparseVector<T> {

		private Map<Long, T> values = new HashMap<Long, T>();

		public T get(long i) {
			return this.values.get(i);
		}

		public void set(long i, T value) {
			this.values.put(i, value);
		}

		public Set<Long> indexes() {
			return this.values.keySet();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((values == null) ? 0 : values.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			SparseVector other = (SparseVector) obj;
			if (values == null) {
				if (other.values != null)
					return false;
			} else if (!values.equals(other.values))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "SparseVector [values=" + values + "]";
		}

	}
}
