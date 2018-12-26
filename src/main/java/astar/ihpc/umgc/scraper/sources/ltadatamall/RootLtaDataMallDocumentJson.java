package astar.ihpc.umgc.scraper.sources.ltadatamall;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The root type for all LTA DataMall results.
 * @author othmannb
 *
 * @param <T>
 */
public abstract class RootLtaDataMallDocumentJson<T>{
	@JsonProperty("odata.metadata")
	private String odataMetadata;
	@JsonProperty("value")
	private T value;
	protected abstract T initialValue();
	
	public String getOdataMetadata() {
		return odataMetadata;
	}
	public void setOdataMetadata(String odataMetadata) {
		this.odataMetadata = odataMetadata;
	}
	public T getValue() {
		return value;
	}
	public void setValue(T value) {
		this.value = value;
	}
}