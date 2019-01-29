package astar.ihpc.umgc.scraper.sources.ltadatamall;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * A {@link RootDataMallDocumentJson} that can be used for ANY Lta DataMall result, storing all data in generic JSONNodes.
 * For example, if you are just simply saving the JSON and not doing any analysis.
 * <p>
 * If there is an error, the LTA DataMall result will be a "fault" object which cannot be cast to this type.
 * <p>
 * If there are any additional properties in the document, it will be defined in {@link #getAdditionalProperties()}.
 * <p>
 * Note: If the value field is present but is not a list (as expected), then it will be set as an additional property (of key "value").
 * <p>
 * This class uses a custom JSON deserializer and serializer to ensure portability and correctness, especially when handling all possible cases.
 * @author othmannb
 *
 */
@JsonDeserialize(using=WhiteBoxLtaDataMallDocumentJson.WhiteBoxLtaDataMallDocumentJsonDeserializer.class)
@JsonSerialize(using=WhiteBoxLtaDataMallDocumentJson.WhiteBoxLtaDataMallDocumentJsonSerializer.class)
public class WhiteBoxLtaDataMallDocumentJson extends BaseLtaDataMallDocumentJson<JsonNode> {
	protected Map<String, JsonNode> additionalProperties = null;
	protected boolean valueEncoded = true;
	public Map<String, JsonNode> getAdditionalProperties() {
		if (additionalProperties == null) {
			additionalProperties = new LinkedHashMap<>();
		}
		return additionalProperties;
	}
	
	public void setAdditionalProperty(String key, JsonNode value) {
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
	
	protected static class WhiteBoxLtaDataMallDocumentJsonDeserializer extends JsonDeserializer<WhiteBoxLtaDataMallDocumentJson> {
		@Override
		public WhiteBoxLtaDataMallDocumentJson deserialize(JsonParser p, DeserializationContext ctxt)
				throws IOException, JsonProcessingException {
			JsonNode root = p.getCodec().readTree(p);
			WhiteBoxLtaDataMallDocumentJson out = new WhiteBoxLtaDataMallDocumentJson();
			boolean hasOdataMetadata = false, valueEncoded = false;
			for (Iterator<Entry<String, JsonNode>> it = root.fields(); it.hasNext(); ) {
				Entry<String, JsonNode> e = it.next();
				String name = e.getKey();
				JsonNode v = e.getValue();
				switch (name) {
				case "odata.metadata":
					String odataMetadata = v.textValue();
					if (odataMetadata == null) {
						throw new JsonParseException(p, "odata.metadata is not a string");
					}
					out.setOdataMetadata(odataMetadata);
					hasOdataMetadata = true;
					break;
				case "value":
					if (v.isArray()) {
						//Traditional value object.
						for (Iterator<JsonNode> it2 = v.elements(); it2.hasNext(); ) {
							out.getValue().add(it2.next());
						}
					} else {
						//Hmm, just put as additional properties.
						out.setAdditionalProperty("value", v);
					}
					valueEncoded = true;
					break;
				case "fault":
					throw new JsonParseException(p, "Result is a DataMall fault object. Cannot be cast to WhiteBoxDataMallDocumentJson.");
				default:
					//Put as additional properties
					out.setAdditionalProperty(name, v);
				}
			}
			if (!hasOdataMetadata) {
				throw new JsonParseException(p, "odata.metadata field required and is missing");
			}
			out.setValueEncoded(valueEncoded);
			return out;
		}
	}
	protected static class WhiteBoxLtaDataMallDocumentJsonSerializer extends JsonSerializer<WhiteBoxLtaDataMallDocumentJson> {
		@Override
		public void serialize(WhiteBoxLtaDataMallDocumentJson value, JsonGenerator gen, SerializerProvider serializers)
				throws IOException {
			gen.writeStartObject();
			gen.writeStringField("odata.metadata", value.getOdataMetadata());
			if (value.isValueEncoded()) {
				if (value.additionalProperties == null || !value.additionalProperties.containsKey("value")) {
					gen.writeObjectField("value", value.getValue());
				}
			}
				
			if (value.additionalProperties != null) {
				for (Entry<String, JsonNode> e : value.additionalProperties.entrySet()) {
					gen.writeObjectField(e.getKey(), e.getValue());
				}
			}
			gen.writeEndObject();
		}
	}
}
