package com.github.pmerienne.trident.state.cassandra;

import static com.github.pmerienne.trident.state.serializer.SerializerFactory.SerializerType.HUMAN_READABLE;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Rule;

import storm.trident.state.Serializer;

import com.datastax.driver.core.Session;
import com.github.pmerienne.trident.state.MapMultimapStateTest;
import com.github.pmerienne.trident.state.cassandra.dao.CassandraDao;
import com.github.pmerienne.trident.state.cassandra.embedded.CQLResource;
import com.github.pmerienne.trident.state.cassandra.embedded.CQLResourceBuilder;
import com.github.pmerienne.trident.state.serializer.SerializerFactory;
import com.github.pmerienne.trident.state.serializer.SerializerFactory.SerializerType;
import com.github.pmerienne.trident.state.testing.TestValue;

public class CassandraMapMultiMapStateTest extends MapMultimapStateTest {

	@Rule
	public CQLResource resource = CQLResourceBuilder.noTruncate();

	private Session session = resource.getNativeSession();

	private CassandraDao dao = new CassandraDao(session);

	private Serializer<String> subKeySerializer = SerializerFactory.<String> createSerializer(SerializerType.BINARY);
	private Serializer<TestValue> serializer = SerializerFactory.<TestValue> createSerializer(HUMAN_READABLE);

	@Before
	public void setup() {
		this.state = new CassandraMapMultiMapState<String, String, TestValue>(RandomStringUtils.randomAlphabetic(6),
				subKeySerializer, serializer, dao.getMapMultiMapQueryTemplate());
	}
}
