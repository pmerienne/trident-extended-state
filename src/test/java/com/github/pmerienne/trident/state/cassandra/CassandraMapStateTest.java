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
package com.github.pmerienne.trident.state.cassandra;

import static com.github.pmerienne.trident.state.serializer.SerializerFactory.SerializerType.HUMAN_READABLE;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import storm.trident.state.Serializer;

import com.datastax.driver.core.Session;
import com.github.pmerienne.trident.state.cassandra.dao.CassandraDao;
import com.github.pmerienne.trident.state.cassandra.embedded.CQLResource;
import com.github.pmerienne.trident.state.cassandra.embedded.CQLResourceBuilder;
import com.github.pmerienne.trident.state.serializer.SerializerFactory;

@RunWith(MockitoJUnitRunner.class)
public class CassandraMapStateTest {

	@Rule
	public CQLResource resource = CQLResourceBuilder.noTruncate();

	private Session session = resource.getNativeSession();

	private CassandraDao dao = new CassandraDao(session);

	private Serializer<Object> jsonSerializer = SerializerFactory.<Object> createSerializer(HUMAN_READABLE);
	private Serializer<Long> serializer = SerializerFactory.<Long> createSerializer(HUMAN_READABLE);

	private CassandraMapState<Long> state;

	@Before
	public void setup() {
		this.state = new CassandraMapState<Long>(RandomStringUtils.randomAlphabetic(6), serializer, jsonSerializer,
				dao.getMapQueryTemplate());
	}

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
