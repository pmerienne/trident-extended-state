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

import static com.github.pmerienne.trident.state.testing.TestValue.random;
import static org.apache.commons.lang.RandomStringUtils.random;
import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.github.pmerienne.trident.state.testing.TestValue;

public abstract class MapStateTest {

	protected MapState<String, TestValue> state;

	@Test
	public void testMultiGetAndPut() {
		// Given
		Map<String, TestValue> expectedValues = new HashMap<String, TestValue>();
		expectedValues.put(random(5), random());
		expectedValues.put(random(5), random());
		expectedValues.put(random(5), random());
		expectedValues.put(random(5), random());

		List<String> keys = new ArrayList<String>(expectedValues.keySet());

		// When
		state.multiPut(expectedValues);
		List<TestValue> actualValues = state.multiGet(keys);

		// Then
		for (int i = 0; i < keys.size(); i++) {
			String key = keys.get(i);
			TestValue expectedValue = expectedValues.get(key);
			TestValue actualValue = actualValues.get(i);
			assertThat(actualValue).isEqualTo(expectedValue);
		}
	}

	@Test
	public void testMultiGetWithNoValues() {
		// Given
		List<String> keys = Arrays.asList(random(5), random(5), random(5));

		// When
		List<TestValue> actualValues = state.multiGet(keys);

		// Then
		assertThat(actualValues).containsExactly(null, null, null);
	}

}
