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

import static org.fest.assertions.Assertions.assertThat;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;

import com.github.pmerienne.trident.state.SparseMatrixState.SparseVector;
import com.github.pmerienne.trident.state.testing.TestValue;

public abstract class SparseMatrixStateTest {

	protected SparseMatrixState<TestValue> state;

	@Test
	public void should_set_and_get_value() {
		// Given
		TestValue expectedValue = TestValue.random();
		long i = RandomUtils.nextInt();
		long j = RandomUtils.nextInt();

		// When
		this.state.set(i, j, expectedValue);
		TestValue actualValue = this.state.get(i, j);

		// Then
		assertThat(actualValue).isEqualTo(expectedValue);
	}

	@Test
	public void should_retrieve_column() {
		// Given
		long i = RandomUtils.nextInt();
		TestValue expectedValue1 = TestValue.random();
		TestValue expectedValue2 = TestValue.random();
		TestValue expectedValue3 = TestValue.random();
		long j1 = RandomUtils.nextInt();
		long j2 = RandomUtils.nextInt();
		long j3 = RandomUtils.nextInt();

		this.state.set(i, j1, expectedValue1);
		this.state.set(i, j2, expectedValue2);
		this.state.set(i, j3, expectedValue3);

		// When
		SparseVector<TestValue> actualColumn = this.state.getColumn(i);

		// Then
		SparseVector<TestValue> expectedColumn = new SparseVector<TestValue>();
		expectedColumn.set(j1, expectedValue1);
		expectedColumn.set(j2, expectedValue2);
		expectedColumn.set(j3, expectedValue3);

		assertThat(SparseVectorComparator.areEquals(actualColumn, expectedColumn)).isTrue();
	}

	@Test
	public void should_retrieve_row() {
		// Given
		long j = RandomUtils.nextInt();
		TestValue expectedValue1 = TestValue.random();
		TestValue expectedValue2 = TestValue.random();
		TestValue expectedValue3 = TestValue.random();
		long i1 = RandomUtils.nextInt();
		long i2 = RandomUtils.nextInt();
		long i3 = RandomUtils.nextInt();

		this.state.set(i1, j, expectedValue1);
		this.state.set(i2, j, expectedValue2);
		this.state.set(i3, j, expectedValue3);

		// When
		SparseVector<TestValue> actualColumn = this.state.getRow(j);

		// Then
		SparseVector<TestValue> expectedColumn = new SparseVector<TestValue>();
		expectedColumn.set(i1, expectedValue1);
		expectedColumn.set(i2, expectedValue2);
		expectedColumn.set(i3, expectedValue3);

		assertThat(SparseVectorComparator.areEquals(actualColumn, expectedColumn)).isTrue();
	}

	private static class SparseVectorComparator {

		public static <T> boolean areEquals(SparseVector<T> v1, SparseVector<T> v2) {

			EqualsBuilder eq = new EqualsBuilder();
			eq.append(v1.indexes(), v2.indexes());
			for (Long index : v1.indexes()) {
				eq.append(v1.get(index), v2.get(index));
			}

			return eq.isEquals();
		}
	}
}
