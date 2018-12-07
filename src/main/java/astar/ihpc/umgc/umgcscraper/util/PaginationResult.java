package astar.ihpc.umgc.umgcscraper.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.asynchttpclient.Request;
import org.asynchttpclient.Response;

public class PaginationResult<T> {
	private final Map<Integer, Integer> pageNumberMapping = new LinkedHashMap<>();
	private final List<Integer> pageNumbers;
	private final List<ScraperResult<T>> scraperResults;
	private final List<byte[]> responseBytes = new ArrayList<>();
	private final List<T> responseData = new ArrayList<>();
	private final List<Integer> retryCounts;
	private final int retryCountTotal;
	private final long createTimeMillis;
	private final long completeTimeMillis;
	public PaginationResult(List<Integer> pageNumbers, List<ScraperResult<T>> scraperResults,
			List<Integer> retryCounts, int retryCountTotal, long createTimeMillis, long completeTimeMillis) {
		super();
		this.pageNumbers = pageNumbers;
		this.scraperResults = scraperResults;
		this.retryCounts = retryCounts;
		this.retryCountTotal = retryCountTotal;
		this.createTimeMillis = createTimeMillis;
		this.completeTimeMillis = completeTimeMillis;
		for (int i = 0; i < pageNumbers.size(); i++) {
			pageNumberMapping.put(pageNumbers.get(i), i);
			ScraperResult<T> res = scraperResults.get(i);
			responseBytes.add(res.getResponseBytes());
			responseData.add(res.getResponseData());
		}
	}
	
	public int size() {
		return pageNumbers.size();
	}
	
	public int getPageNumber(int index) {
		return pageNumbers.get(index);
	}
	
	public int indexOf(int pageNumber) {
		Integer result = pageNumberMapping.get(pageNumber);
		if (result == null) return -1;
		return result;
	}
	
	public ScraperResult<T> getScraperResult(int index){
		return scraperResults.get(index);
	}
	
	public Request getRequest(int index) {
		return getScraperResult(index).getRequest();
	}
	
	public Response getResponse(int index) {
		return getScraperResult(index).getResponse();
	}

	public byte[] getResponseBytes(int index) {
		return getScraperResult(index).getResponseBytes();
	}
	
	public T getResponseData(int index) {
		return getScraperResult(index).getResponseData();
	}

	public List<byte[]> getResponseBytes(){
		return responseBytes;
	}

	public List<T> getResponseData(){
		return responseData;
	}
	
	public List<ScraperResult<T>> getScraperResults(){
		return scraperResults;
	}
	
	public int getRetryCount(int index) {
		return retryCounts.get(index);
	}
	
	public int getRetryCountTotal() {
		return retryCountTotal;
	}
	
	public long getCreateTimeMillis() {
		return createTimeMillis;
	}
	
	public long getCompleteTimeMillis() {
		return completeTimeMillis;
	}
}
