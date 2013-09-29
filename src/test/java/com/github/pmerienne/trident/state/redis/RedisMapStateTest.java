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
package com.github.pmerienne.trident.state.redis;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RedisMapStateTest {

	private RedisMapState<Long> state;

	@Before
	public void setup() {
		this.state = new RedisMapState<Long>("test");
	}

	@After
	public void cleanup() {
		this.state.flushAll();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMultiGetAndUpdateWithCompoundKeys() {
		// Given
		Object key1 = 123;
		Object key2 = 456;
		List<List<Object>> keys = Arrays.asList(Arrays.asList(key1, key2));
		List<Long> expectedValues = Arrays.asList(12L);

		// When
		this.state.multiPut(keys, expectedValues);

		// Then
		List<Long> actualValues = this.state.multiGet(keys);
		assertEquals(expectedValues, actualValues);

	}
}
