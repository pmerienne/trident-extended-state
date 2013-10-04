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

public interface SparseMatrixState<T> extends ExtendedState<T> {

	/**
	 * Gets a specific value inside the matrix.
	 * 
	 * @param i
	 *            row index
	 * @param j
	 *            column index
	 * @return the specific value or <code>null</code> if no value was found
	 */
	T get(long i, long j);

	/**
	 * Sets a specific value inside the matrix.
	 * 
	 * @param i
	 *            row index
	 * @param j
	 *            column index
	 * @param value
	 *            the new value
	 */
	void set(long i, long j, T value);

	/**
	 * Gets a specific column as a {@link SparseVector}.
	 * 
	 * @param i
	 *            column index
	 * @return the specific row
	 */
	SparseVector<T> getColumn(long i);

	/**
	 * Gets a specific row as a {@link SparseVector}.
	 * 
	 * @param j
	 *            row index
	 * @return the specific row
	 */
	SparseVector<T> getRow(long j);

	public static interface SparseVector<T> extends Serializable {

		/**
		 * Gets a specific value
		 * 
		 * @param i
		 *            value index
		 * @return the value or <code>null</code> if no value was found
		 */
		T get(long i);

		/**
		 * Sets/Replace a specific value
		 * 
		 * @param i
		 *            value index
		 * @param value
		 *            the new value
		 */
		void set(long i, T value);

		/**
		 * Returns a {@link Set} of the present indexes (ordered ASC)
		 * 
		 * @return
		 */
		Set<Long> indexes();

	}
}
