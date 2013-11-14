package com.github.pmerienne.trident.state.cassandra;

import static com.github.pmerienne.trident.state.serializer.SerializerFactory.SerializerType.HUMAN_READABLE;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Rule;

import storm.trident.state.Serializer;

import com.datastax.driver.core.Session;
import com.github.pmerienne.trident.state.SetMultiMapStateTest;
import com.github.pmerienne.trident.state.cassandra.dao.CassandraDao;
import com.github.pmerienne.trident.state.cassandra.embedded.CQLResource;
import com.github.pmerienne.trident.state.cassandra.embedded.CQLResourceBuilder;
import com.github.pmerienne.trident.state.serializer.SerializerFactory;
import com.github.pmerienne.trident.state.testing.TestValue;

public class CassandraSetMultiMapStateTest extends SetMultiMapStateTest {

	@Rule
	public CQLResource resource = CQLResourceBuilder.noTruncate();

	private Session session = resource.getNativeSession();

	private CassandraDao dao = new CassandraDao(session);

	private Serializer<TestValue> serializer = SerializerFactory.<TestValue> createSerializer(HUMAN_READABLE);

	@Before
	public void setup() {
		this.state = new CassandraSetMultiMapState<String, TestValue>(RandomStringUtils.randomAlphabetic(6),
				serializer, dao.getSetMultiMapQueryTemplate());
	}
}
