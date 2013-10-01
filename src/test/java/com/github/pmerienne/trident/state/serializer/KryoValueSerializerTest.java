package com.github.pmerienne.trident.state.serializer;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

import com.github.pmerienne.trident.state.testing.TestValue;

public class KryoValueSerializerTest {

	@Test
	public void should_serialize_and_deserialize_without_loss() {
		// Given
		KryoValueSerializer<TestValue> serializer = new KryoValueSerializer<TestValue>();
		TestValue expectedObject = TestValue.random();

		// When
		byte[] serialized = serializer.serialize(expectedObject);
		TestValue deserialized = serializer.deserialize(serialized);

		// Then
		assertThat(deserialized).isEqualTo(expectedObject);
	}

}
