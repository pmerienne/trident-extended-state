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

import static com.github.pmerienne.trident.state.serializer.SerializerFactory.SerializerType.BINARY;
import static com.github.pmerienne.trident.state.serializer.SerializerFactory.SerializerType.HUMAN_READABLE;
import static com.github.pmerienne.trident.state.serializer.SerializerFactory.createSerializer;
import static org.fest.assertions.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.serialization.DefaultKryoFactory;

public class SerializerFactoryTest {

	private Config config;

	@Before
	public void createStormConfiguration() {
		config = new Config();
		config.setKryoFactory(DefaultKryoFactory.class);
		config.setFallBackOnJavaSerialization(true);
		config.setSkipMissingKryoRegistrations(false);
	}

	@Test
	public void should_instanciate_kryo_serializer_for_binary_serializer() {
		assertThat(createSerializer(BINARY)).isInstanceOf(KryoValueSerializer.class);
		assertThat(createSerializer(BINARY, config)).isInstanceOf(KryoValueSerializer.class);
	}

	@Test
	public void should_instanciate_json_serializer_for_human_readable_serializer() {
		assertThat(createSerializer(HUMAN_READABLE)).isInstanceOf(JsonValueSerializer.class);
		assertThat(createSerializer(HUMAN_READABLE, config)).isInstanceOf(JsonValueSerializer.class);
	}
}
