To build a `CassandraConfig` object :

```java

	CassandraConfig config = CassandraConfig.builder()
			.setContactPoints(<IP_address>)
			.setPort(<native_transport_port>)
			.setKeyspace(<your_keyspace_name>).build();
	
```

Example of `CassandraConfig` for a local Cassandra server (assuming that native_transport_port is set using default value 9042)

```java

	CassandraConfig config = CassandraConfig.builder()
			.setContactPoints("localhost")
			.setPort(9042)
			.setKeyspace("trident_ex_state").build();
	
```
