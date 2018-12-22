package astar.ihpc.umgc.scraper.sources.ltadatamall;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * A {@link RootLtaDataMallDocumentJson} that can be used for ANY Lta DataMall result.
 * This should only be used if you don't care about the content/value of the document,
 * and just want to ensure that the result was retrieved successfully.
 * For example, if you are just simply saving the JSON and not doing any analysis.
 * <p>
 * If there is an error, the LTA DataMall result will be a "fault" object which cannot be cast to this type.
 * @author othmannb
 *
 */
public class UniversalLtaDataMallDocumentJson extends RootLtaDataMallDocumentJson<JsonNode> {
	private Map<String, Object> additionalProperties = new LinkedHashMap<>();
	@Override
	protected JsonNode initialValue() {
		return new ArrayNode(JsonNodeFactory.instance);
	}
	
	@JsonAnyGetter
	public Map<String, Object> getAdditionalProperties() {
		return additionalProperties;
	}
	
	@JsonAnySetter
	public void setAdditionalProperty(String key, Object value) {
		if (key.equals("fault")) {
			throw new IllegalArgumentException("fault is not allowed as a key");
		}
		additionalProperties.put(key, value);
	}

}
