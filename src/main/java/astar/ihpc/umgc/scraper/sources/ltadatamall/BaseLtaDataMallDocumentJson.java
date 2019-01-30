package astar.ihpc.umgc.scraper.sources.ltadatamall;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An abstract document corresponding to the LTA DataMall JSON response object.
 * The data values for the response is located in the "value" field ({@link #getValue()}), 
 * which is an array of records. 
 * <p>
 * Each distinct dataset in LTA DataMall returns a different type of record.
 * <p>
 * Therefore to deserialize the LTA response into a concrete type, you must first construct a corresponding
 * document class extending this class and specifying the concrete type of value. You can then deserialize using
 * the new concrete document class as the object-mapped type.
 * <p>
 * Do not try to deserialize directly using this abstract class, since Java will not know what type of record to read.
 * <p>
 * Almost every LTA DataMall API result can be directly deserialized into this type, since virtually all of them comform to this structure.
 * However, there are exceptions. For those exceptions, you can use the {@link WhiteBoxLtaDataMallDocumentJson} which can support additional properties and non-list values.
 * <p>
 * If you do not care about the type of the object, you can use the {@link WhiteBoxLtaDataMallDocumentJson} class which can support ALL APIs, but doesn't do full object-mapping (each record is
 * not converted to a POJO).
 * @author othmannb
 *
 * @param <T>
 */
public abstract class BaseLtaDataMallDocumentJson<T> {
	@JsonProperty("odata.metadata")
	private String odataMetadata;
	@JsonProperty("value")
	private List<T> value = new ArrayList<T>();
	
	public String getOdataMetadata() {
		return odataMetadata;
	}
	public void setOdataMetadata(String odataMetadata) {
		this.odataMetadata = odataMetadata;
	}
	public List<T> getValue() {
		return value;
	}
	public void setValue(List<T> value) {
		this.value = value;
	}
}