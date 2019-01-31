package org.umgc.umgcscraper;

import astar.ihpc.umgc.scraper.sources.ltadatamall.*;
import java.io.IOException;
import java.util.AbstractList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * A {@link RootDataMallDocumentJson} that can be used for ANY Lta DataMall result, but discarding all values to save space.
 * This should only be used if you don't care about the content/value of the document,
 * and just want to ensure that the result was retrieved successfully.
 * For example, if you are just simply saving the JSON and not doing any analysis.
 * <p>
 * If there is an error, the LTA DataMall result will be a "fault" object which cannot be cast to this type.
 * <p>
 * If there are any additional properties in the document, it will be defined in {@link #getAdditionalProperties()}.
 * <p>
 * Note: All values in the document are lost, except how many records and what the keys of the additional properties are.
 * This is to reduce memory consumption by this object.
 * This is very useful if you only want to know if the document is valid, and how many records it contains. 
 * @author othmannb
 *
 */
@JsonDeserialize(using=BlackBoxLtaDataMallDocumentJson.BlackBoxLtaDataMallDocumentJsonDeserializer.class)
@JsonSerialize(using=BlackBoxLtaDataMallDocumentJson.BlackBoxLtaDataMallDocumentJsonSerializer.class)
public class BlackBoxLtaDataMallDocumentJson extends BaseLtaDataMallDocumentJson<Object> {
	private int nValues = 0;
	public BlackBoxLtaDataMallDocumentJson() {
		setValue(new AbstractList<Object>() {
			@Override
			public Object get(int index) {
				if (index < 0 || index > nValues) throw new IndexOutOfBoundsException();
				return BlackHoleValue.INSTANCE;
			}

			@Override
			public int size() {
				return nValues;
			}
			
		});
	}
	public static class BlackHoleValue {
		public static BlackHoleValue INSTANCE = new BlackHoleValue();
		private BlackHoleValue() {}
		@Override
		public String toString() {
			return "BlackHoleValue";
		}
		@Override
		public int hashCode() {
			return 0;
		}
		@Override
		public boolean equals(Object obj) {
			return obj == this;
		}
	}
	
	
	protected Map<String, Object> additionalProperties = null;
	protected boolean valueEncoded = true;
	public Map<String, Object> getAdditionalProperties() {
		if (additionalProperties == null) {
			additionalProperties = new LinkedHashMap<>();
		}
		return additionalProperties;
	}
	
	public void setAdditionalProperty(String key, Object value) {
		if (key.equals("fault")) {
			throw new IllegalArgumentException("fault is not allowed as a key");
		}
		if (additionalProperties == null) {
			additionalProperties = new LinkedHashMap<>();
		}
		additionalProperties.put(key, value);
	}
	
	/**
	 * Does the document have a value field set?
	 * @return
	 */
	public boolean isValueEncoded() {
		return valueEncoded;
	}
	public void setValueEncoded(boolean valueEncoded) {
		this.valueEncoded = valueEncoded;
	}
	
	protected static class BlackBoxLtaDataMallDocumentJsonDeserializer extends JsonDeserializer<BlackBoxLtaDataMallDocumentJson> {
		private final static void blackHoleObject(JsonParser p) throws IOException{
			JsonToken tok = null;
			while ((tok = p.nextToken()) != JsonToken.END_OBJECT) {
				if (tok == JsonToken.START_OBJECT) {
					blackHoleObject(p);
				} else if (tok == JsonToken.START_ARRAY) {
					blackHoleArray(p);
				} else if (tok == null) {
					return;
				}
			}
		}
		private final static void blackHoleArray(JsonParser p) throws IOException{
			JsonToken tok = null;
			while ((tok = p.nextToken()) != JsonToken.END_ARRAY) {
				if (tok == JsonToken.START_OBJECT) {
					blackHoleObject(p);
				} else if (tok == JsonToken.START_ARRAY) {
					blackHoleArray(p);
				} else if (tok == null) {
					return;
				}
			}
		}
		
		@Override
		public BlackBoxLtaDataMallDocumentJson deserialize(JsonParser p, DeserializationContext ctxt)
				throws IOException, JsonProcessingException {
			BlackBoxLtaDataMallDocumentJson out = new BlackBoxLtaDataMallDocumentJson();
			JsonToken tok = p.currentToken();
			if (tok != JsonToken.START_OBJECT) {
				throw new JsonParseException(p, "Excepted start of object");
			}
			boolean hasOdataMetadata = false, valueEncoded = false;
			int nValues = 0;
			while ((tok = p.nextToken()) != JsonToken.END_OBJECT) {
				//Read the field.
				if (tok != JsonToken.FIELD_NAME) {
					throw new JsonParseException(p, "Expected field name");
				}
				tok = p.nextToken();
				if (tok == null) {
					throw new JsonParseException(p, "Expected field value");
				}
				
				switch (p.getCurrentName()) {
				case "odata.metadata":
					if (tok == JsonToken.VALUE_STRING) {
						out.setOdataMetadata(p.getValueAsString());
					} else {
						throw new JsonParseException(p, "Expected odata.metadata to be a string");
					}
					hasOdataMetadata = true;
					break;
				case "fault":
					throw new JsonParseException(p, "Result is a DataMall fault object. Cannot be cast to BlackBoxDataMallDocumentJson.");
				case "value":
					if (tok == JsonToken.START_ARRAY) {
						while ((tok = p.nextToken()) != JsonToken.END_ARRAY) {
							nValues++;
							if (tok == JsonToken.START_ARRAY) {
								blackHoleArray(p);
							} else if (tok == JsonToken.START_OBJECT) {
								blackHoleObject(p);
							} else if (tok == null) {
								throw new JsonParseException(p, "Excpected valid token");
							}
						}
					} else {
						if (tok == JsonToken.START_OBJECT) {
							blackHoleObject(p);
						}
						out.setAdditionalProperty("value", BlackHoleValue.INSTANCE);
					}
					valueEncoded = true;
					break;
				default:
					if (tok == JsonToken.START_ARRAY) {
						blackHoleArray(p);
					} else if (tok == JsonToken.START_OBJECT) {
						blackHoleObject(p);
					}
					//Put as additional properties
					out.setAdditionalProperty(p.getCurrentName(), BlackHoleValue.INSTANCE);
				}
			}
			if (!hasOdataMetadata) {
				throw new JsonParseException(p, "odata.metadata field required and is missing");
			}
			out.setValueEncoded(valueEncoded);
			out.nValues = nValues;
			return out;
		}
	}
	protected static class BlackBoxLtaDataMallDocumentJsonSerializer extends JsonSerializer<BlackBoxLtaDataMallDocumentJson> {
		@Override
		public void serialize(BlackBoxLtaDataMallDocumentJson value, JsonGenerator gen, SerializerProvider serializers)
				throws IOException {
			gen.writeStartObject();
			gen.writeStringField("odata.metadata", value.getOdataMetadata());
			if (value.isValueEncoded()) {
				if (value.additionalProperties == null || !value.additionalProperties.containsKey("value")) {
					gen.writeObjectField("value", value.getValue());
				}
			}
				
			if (value.additionalProperties != null) {
				for (Entry<String, Object> e : value.additionalProperties.entrySet()) {
					gen.writeObjectField(e.getKey(), e.getValue());
				}
			}
			gen.writeEndObject();
		}
	}
}
