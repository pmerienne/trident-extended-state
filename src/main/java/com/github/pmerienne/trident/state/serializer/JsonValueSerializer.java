package com.github.pmerienne.trident.state.serializer;

import java.lang.reflect.Type;

import storm.trident.state.Serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class JsonValueSerializer<T> implements Serializer<T> {

	private static final long serialVersionUID = -2910589120089169978L;

	private final Gson gson;

	public JsonValueSerializer() {
		this.gson = new GsonBuilder().registerTypeAdapter(Wrapper.class, new JSONWithClassAdapater<T>()).create();
	}

	@Override
	public byte[] serialize(T obj) {
		String json = this.gson.toJson(new Wrapper<T>(obj));
		return json.getBytes();
	}

	@SuppressWarnings("unchecked")
	@Override
	public T deserialize(byte[] bytes) {
		String json = new String(bytes);
		Wrapper<T> wrapper = this.gson.fromJson(json, Wrapper.class);
		return wrapper.value;
	}

	public static class JSONWithClassAdapater<T> implements JsonSerializer<Wrapper<T>>, JsonDeserializer<Wrapper<T>> {

		private static final String CLASSNAME_FIELD = "class";
		private static final String VALUE_FIELD = "value";

		@SuppressWarnings("unchecked")
		public Wrapper<T> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws com.google.gson.JsonParseException {
			JsonObject jsonObject = json.getAsJsonObject();

			JsonElement classnameElement = jsonObject.get(CLASSNAME_FIELD);
			JsonElement valueElement = jsonObject.get(VALUE_FIELD);

			Class<T> klass = null;
			try {
				klass = (Class<T>) Class.forName(classnameElement.getAsString());
			} catch (ClassNotFoundException e) {
				throw new com.google.gson.JsonParseException(e.getMessage());
			}

			T value = context.deserialize(valueElement, klass);

			return new Wrapper<T>(value);
		}

		public JsonElement serialize(Wrapper<T> src, Type typeOfSrc, JsonSerializationContext context) {
			JsonElement classnameElement = context.serialize(src.value.getClass().getName());
			JsonElement valueElement = context.serialize(src.value);

			JsonObject jsonObject = new JsonObject();
			jsonObject.add(CLASSNAME_FIELD, classnameElement);
			jsonObject.add(VALUE_FIELD, valueElement);

			return jsonObject;
		}

		protected Class<?> getClassFromTypedValueJson(JsonObject json) {
			Class<?> klass = null;
			try {
				klass = Class.forName(json.get("className").getAsString());
			} catch (ClassNotFoundException e) {
				throw new com.google.gson.JsonParseException(e.getMessage());
			}

			return klass;
		}
	}

	public static class Wrapper<T> {

		public T value;

		public Wrapper() {
		}

		public Wrapper(T value) {
			this.value = value;
		}
	}
}
