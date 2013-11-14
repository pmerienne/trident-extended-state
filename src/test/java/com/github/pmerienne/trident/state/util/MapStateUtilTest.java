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
package com.github.pmerienne.trident.state.util;

import static com.github.pmerienne.trident.state.util.MapStateUtil.toKeys;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

import storm.trident.state.map.MapState;

import com.github.pmerienne.trident.state.testing.TestValue;

@SuppressWarnings("unchecked")
public class MapStateUtilTest {

	@Test
	public void should_convert_single_key_to_storm_keys() {
		// Given
		String key = randomUUID().toString();

		// When
		List<List<Object>> keys = MapStateUtil.toKeys(key);

		// Then
		assertThat(keys).hasSize(1);
		assertThat(keys.get(0)).hasSize(1);
		assertThat(keys.get(0).get(0)).isEqualTo(key);
	}

	@Test
	public void should_extract_single_value_from_list() {
		// Given
		TestValue expectedValue = TestValue.random();
		List<TestValue> values = asList(expectedValue);

		// When
		TestValue actualValue = MapStateUtil.singleValue(values);

		// Then
		assertThat(actualValue).isEqualTo(expectedValue);
	}

	@Test
	public void should_extract_single_value_from_empty_list() {
		// Given
		List<TestValue> values = new ArrayList<TestValue>();

		// When
		TestValue actualValue = MapStateUtil.singleValue(values);

		// Then
		assertThat(actualValue).isNull();
	}

	@Test
	public void should_extract_single_value_from_null_list() {
		// When
		TestValue actualValue = MapStateUtil.singleValue(null);

		// Then
		assertThat(actualValue).isNull();
	}

	@Test
	public void should_put_single_value() {
		// Given
		String key = randomUUID().toString();
		TestValue value = TestValue.random();

		MapState<TestValue> state = mock(MapState.class);

		// When
		MapStateUtil.putSingle(state, key, value);

		// Then
		verify(state).multiPut(toKeys(key), asList(value));
	}

	@Test
	public void should_put_single_value_with_2_keys() {
		// Given
		Object key1 = randomUUID().toString();
		Object key2 = randomUUID().toString();
		TestValue value = TestValue.random();

		MapState<TestValue> state = mock(MapState.class);

		// When
		MapStateUtil.putSingle(state, key1, key2, value);

		// Then
		verify(state).multiPut(asList(asList(key1, key2)), asList(value));
	}

	@Test
	public void should_get_single_value_with_2_keys() {
		// Given
		Object key1 = randomUUID().toString();
		Object key2 = randomUUID().toString();
		TestValue expectedValue = TestValue.random();

		MapState<TestValue> state = mock(MapState.class);
		Mockito.when(state.multiGet(asList(asList(key1, key2)))).thenReturn(asList(expectedValue));

		// When
		TestValue actualValue = MapStateUtil.getSingle(state, key1, key2);

		// Then
		assertThat(actualValue).isEqualTo(expectedValue);
	}
}
