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
package com.github.pmerienne.trident.state.serializer;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

import com.github.pmerienne.trident.state.testing.TestValue;

public class JsonValueSerializerTest {

	@Test
	public void should_serialize_and_deserialize_without_loss() {
		// Given
		JsonValueSerializer<TestValue> serializer = new JsonValueSerializer<TestValue>();
		TestValue expectedObject = TestValue.random();

		// When
		byte[] serialized = serializer.serialize(expectedObject);
		TestValue deserialized = serializer.deserialize(serialized);

		// Then
		assertThat(deserialized).isEqualTo(expectedObject);
	}

}
