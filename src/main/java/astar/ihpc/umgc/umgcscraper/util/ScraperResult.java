package astar.ihpc.umgc.umgcscraper.util;

import java.util.Arrays;

import org.asynchttpclient.Request;
import org.asynchttpclient.Response;

/**
 * The result from the {@link ScraperClient}. This is the completed result of the scraper client. If you can receive this result, it means the request is successful and
 * the response content is received in full.
 * @author othmannb
 *
 * @param <T>
 */
public class ScraperResult<T> {
	private final Request request;
	private final Response response;
	private final byte[] responseBytes;
	private final T responseData;
	private final long createTimeMillis, requestTimeMillis, responseTimeMillis, completeTimeMillis;
	public ScraperResult(Request request, Response response, byte[] responseBytes, T responseData,
			long createTimeMillis, long requestTimeMillis, long responseTimeMillis, long completeTimeMillis) {
		super();
		this.request = request;
		this.response = response;
		this.responseBytes = responseBytes;
		this.responseData = responseData;
		this.createTimeMillis = createTimeMillis;
		this.requestTimeMillis = requestTimeMillis;
		this.responseTimeMillis = responseTimeMillis;
		this.completeTimeMillis = completeTimeMillis;
	}
	
	/**
	 * The HTTP request sent to the server.
	 * @return
	 */
	public Request getRequest() {
		return request;
	}
	/**
	 * The HTTP response received from the server.
	 * @return
	 */
	public Response getResponse() {
		return response;
	}
	/**
	 * The response content as a byte array.
	 * @return
	 */
	public byte[] getResponseBytes() {
		return responseBytes;
	}
	/**
	 * The response content converted to the data type given by {@link #getResponseDataType()}.
	 * @return
	 */
	public T getResponseData() {
		return responseData;
	}
	/**
	 * The time in which the request was first created (when the user of ScraperClient submits the request).
	 * In milliseconds since Unix Epoch.
	 * @return
	 */
	public long getCreateTimeMillis() {
		return createTimeMillis;
	}
	/**
	 * The time in which the request was first dispatched to the server.
	 * In milliseconds since Unix Epoch.
	 * @return
	 */
	public long getRequestTimeMillis() {
		return requestTimeMillis;
	}
	/**
	 * The time in which the response has been fully received from the server.
	 * In milliseconds since Unix Epoch.
	 * @return
	 */
	public long getResponseTimeMillis() {
		return responseTimeMillis;
	}
	
	/**
	 * The time in which the response is fully received and parsed into the data type.
	 * In millseconds since Unix Epoch.
	 * @return
	 */
	public long getCompleteTimeMillis() {
		return completeTimeMillis;
	}

	@Override
	public String toString() {
		return "ScraperResult [request=" + request + ", response=" + response + ", responseBytes="
				+ Arrays.toString(responseBytes) + ", createTimeMillis=" + createTimeMillis + ", requestTimeMillis=" + requestTimeMillis
				+ ", responseTimeMillis=" + responseTimeMillis + ", completeTimeMillis=" + completeTimeMillis + "]";
	}
	
	

}
