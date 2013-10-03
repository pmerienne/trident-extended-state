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

import java.io.Serializable;
import java.util.Set;

public interface SparseMatrixState<T> {

	T get(long i, long j);

	void set(long i, long j, T value);

	SparseVector<T> getColumn(long i);

	void setColumn(long i, SparseVector<T> column);

	SparseVector<T> getRow(long j);

	void setRow(long i, SparseVector<T> row);

	public static interface SparseVector<T> extends Serializable {

		T get(long i);

		void set(long i, T value);

		Set<Long> indexes();

	}
}
