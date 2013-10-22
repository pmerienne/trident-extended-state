Data structure states for Trident
======================
Trident-extended-state provides data structure [States](https://github.com/nathanmarz/storm/blob/master/storm-core/src/jvm/storm/trident/state/State.java) to Trident.

Unlike traditional MapState where a simple value is mapped to a key, this project lets you operate on more advanced values which expose various operations.
Theses new states have currently a redis and an in memory implementation. They will soon be implemented on cassandra.

### Project location
Primary development of trident-extended-state will take place at https://github.com/pmerienne/trident-extended-state

### Building from Source

		$ mvn install

This command will build and test trident-extended-state. Be aware that redis' tests need a a running redis server on localhost:6379.


### Maven integration : 

Trident-extended-state is hosted on Clojars (a Maven repository). 
To include Trident-ML in your project , add the following to your pom.xml : 
 ```xml
 <repositories>
	<repository>
		<id>clojars.org</id>
		<url>http://clojars.org/repo</url>
	</repository>
</repositories>

<dependency>
  <groupId>com.github.pmerienne</groupId>
  <artifactId>trident-extended-state</artifactId>
  <version>0.0.2</version>
</dependency>
 ```
