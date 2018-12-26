package astar.ihpc.umgc.scraper.sources.ltadatamall;

import java.util.ArrayList;
import java.util.List;

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
 * @author othmannb
 *
 * @param <T>
 */
public abstract class BaseLtaDataMallDocumentJson<T> extends RootLtaDataMallDocumentJson<List<T>>{
	protected List<T> initialValue(){
		return new ArrayList<T>();
	}
}