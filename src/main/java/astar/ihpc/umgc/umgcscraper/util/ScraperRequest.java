package astar.ihpc.umgc.umgcscraper.util;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.asynchttpclient.Request;
import org.asynchttpclient.Response;

/**
 * A package-private request wrapper used by {@link ScraperClient}. Not accessible and useful.
 * @author othmannb
 *
 * @param <T>
 */
class ScraperRequest<T>{
	enum Status {
		QUEUING,
		RUNNING,
		DONE
	}
	private final Request request; 
	private final BiFunction<Request, Response, T> transformer; 
	private final CompletableFuture<ScraperResult<T>> outerFuture;
	//private CompletableFuture<Response> innerFuture;
	private Status status = Status.QUEUING;
	private volatile long createTimeMillis, requestTimeMillis, responseTimeMillis, completeTimeMillis;
	protected volatile boolean innerFutureAbort;
	public ScraperRequest(Request request, BiFunction<Request, Response, T> transformer) {
		super();
		
		this.request = request;
		this.transformer = transformer;
		
		this.outerFuture = new CompletableFuture<>();
		outerFuture.whenComplete(new BiConsumer<ScraperResult<T>,Throwable>(){
			@Override
			public void accept(ScraperResult<T> t, Throwable u) {
				innerFutureAbort = true;
			}
		});
		
		createTimeMillis = System.currentTimeMillis();
	}
	
	public synchronized void advance(CompletableFuture<Response> innerFuture) {
		if (status != Status.DONE) {
			status = Status.RUNNING;
			requestTimeMillis = System.currentTimeMillis();
			innerFuture.whenComplete(new BiConsumer<Response, Throwable>() {
				@Override
				public void accept(Response response, Throwable u) {
					if (response != null) {
						//Successful response.
						responseTimeMillis = System.currentTimeMillis();
						byte[] responseBytes = response.getResponseBodyAsBytes();
						try {
							T responseData = transformer.apply(request, response);
							completeTimeMillis = System.currentTimeMillis();
							ScraperResult<T> result = new ScraperResult<T>(request, response, responseBytes, responseData, createTimeMillis, requestTimeMillis, responseTimeMillis, completeTimeMillis);
							outerFuture.complete(result);
						} catch (Exception e) {
							outerFuture.completeExceptionally(e);
						}
					} else {
						//Error...
						outerFuture.completeExceptionally(u);
					}
				}
			});
		}
	}
	public Request getRequest() {
		return request;
	}
	public BiFunction<Request, Response, T> getTransformer() {
		return transformer;
	}
	
	public synchronized CompletableFuture<ScraperResult<T>> getOuterFuture() {
		return outerFuture;
	}
	public synchronized Status getStatus() {
		return status;
	}
	public synchronized long getCreateTimeMillis() {
		return createTimeMillis;
	}
	public synchronized long getRequestTimeMillis() {
		return requestTimeMillis;
	}
	public synchronized long getResponseTimeMillis() {
		return responseTimeMillis;
	}
	public synchronized long getCompleteTimeMillis() {
		return completeTimeMillis;
	}
	
	public void cancel() {
		outerFuture.cancel(true);
		innerFutureAbort = true;
		synchronized(this) {
			if (status == Status.DONE) {
				//Nothing to do.
				return;
			}
			status = Status.DONE;
		}
	}
	

}
