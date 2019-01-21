package astar.ihpc.umgc.scraper.util;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.asynchttpclient.AsyncCompletionHandlerBase;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.asynchttpclient.config.AsyncHttpClientConfigDefaults;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import astar.ihpc.umgc.scraper.util.ScraperRequest.Status;
import io.netty.handler.codec.http.HttpHeaders;


/**
 * An async HTTP client for scraping HTTP data from public APIs.
 * Under the hood, the client uses the async-http-client library for submitting HTTP requests,
 * and the Jackson library for JSON processing.
 * <p>
 * Beyond those libraries, this class also additionally implements throttling and connection limits (without blocking or throwing exceptions),
 * so that we do not spam a server. The throttling is handled on a per-instance basis. Each client instance has a background thread that does
 * the throttling and the dispatching of requests. You should use one ScrapperClient for each remote host (e.g., DataMall); therefore one background thread per host.
 * <p>
 * You can also add transformations to your request that can additionally convert the retrieved data to a certain data type, or perform other kinds of post-processing.
 * For example, many of the overloaded methods accept a target class type to convert JSON to Java object using Jackson library.
 * <p>
 * Each request returns a {@link CompletableFuture} with a result value of ScraperResult. You can treat the future as a {@link CompletionStage}, and use it to compose
 * a multi-stage pipeline. Or you can treat it like a {@link Future} and use its get() method for blocking get, or cancel() to attempt to cancel the HTTP request (not sending it if possible).
 * <p>
 * It is indeed possible for you to submit 100 requests and then later decide to cancel 10 of them. If some of those requests have not been sent yet, they will be skipped, potentially
 * saving resources.
 * <p>
 * Under typical situations, your program is only scrapping one or two datasets from the same host; thus simply create one instance
 * and retain it for the lifetime of your program.
 * <p>
 * All methods are thread-safe, and most are synchronized. You can submit as many requests you want from as many threads. The object to synchronize with is the ScraperClient itself.
 * <p>
 * When the client is no longer required, call shutdown() to terminate its background thread, as well as cancel all pending requests.
 * @author othmannb
 *
 */
public class ScraperClient implements AutoCloseable{
	private final boolean ownClient;
	private final Thread backgroundThread;
	private volatile boolean shutdown;
	private final int maxConcurrentRequests;
	private final int throttleMillis;
	private final ObjectMapper objectMapper;
	private final ArrayDeque<ScraperRequest<?>> queue = new ArrayDeque<>(256);
	private final Object lock = this;
	private final AsyncHttpClient httpClient;
	
	/**
	 * Create a ScraperClient with supplied dependencies.
	 * Use this constructor if you want to change the behaviour of the JSON parser or the underlying HTTP client.
	 * <p>
	 * The ScraperClient allows you to queue numerous requests at once, and it will process the requests asynchronously in a throttled manner
	 * to avoid overloading the server. It also has several methods to help you transform the HTTP response into something more usable.
	 * <p>
	 * Warning: Closing this client will NOT close() the underlying HTTP client (available from {@link #getHttpClient()}) since the client is provided
	 * by the user, and therefore the user should close the client themselves.
	 * @param maxConcurrentRequests the maximum number of running requests that can be executed at a given time (recommended 4-16)
	 * @param throttleMillis the minimum time between two adjacent requests (recommended 50)
	 * @param objectMapper used to parse JSON into Java objects (from Jackson library)
	 * @param httpClient the asynchronous HTTP client that will do the actual dispatching (from async-http-client library)
	 */
	public ScraperClient(int maxConcurrentRequests, int throttleMillis, ObjectMapper objectMapper, AsyncHttpClient httpClient) {
		this.maxConcurrentRequests = maxConcurrentRequests;
		this.throttleMillis = throttleMillis;
		this.objectMapper = objectMapper;
		this.httpClient = httpClient;
		ownClient = false;
		backgroundThread = createBackgroundThread(null);
		backgroundThread.start();
		
	}
	/**
	 * Create a ScraperClient with default dependencies.
	 * The ObjectMapper will be a plain new ObjectMapper() and the AsyncHttpClient will be a new DefaultAsyncHttpClient().
	 * The AsyncHttpClient will use default configuration, with the following overrides:
	 * connect timeout capped at 5000ms, request timeout capped at 10000ms, connectionTtl to 500ms, pooledConnectionIdleTimeout to 1000ms, maxRequestRetry and maxRedirects to zero.
	 * If the defaults are lower than the caps, they will be used instead. This is because a web scraper needs to process fast, and APIs are expected to be fast.
	 * And we don't want this class to be slow if you forgot to change the defaults.
	 * <p>
	 * The ScraperClient allows you to queue numerous requests at once, and it will process the requests asynchronously in a throttled manner
	 * to avoid overloading the server. It also has several methods to help you transform the HTTP response into something more usable.
	 * <p>
	 * Warning: Closing this client will also close() the underlying HTTP client (available from {@link #getHttpClient()}) since the client is owned
	 * and managed by this class.
	 * @param maxConcurrentRequests the maximum number of running requests that can be executed at a given time (recommended 4-16)
	 * @param throttleMillis the minimum time between two adjacent requests (recommended 50)
	 * @param objectMapper used to parse JSON into Java objects (from Jackson library)
	 * @param httpClient the asynchronous HTTP client that will do the actual dispatching (from async-http-client library)
	 */
	public ScraperClient(int maxConcurrentRequests, int throttleMillis) {
		this.maxConcurrentRequests = maxConcurrentRequests;
		this.throttleMillis = throttleMillis;
		this.objectMapper = ScraperUtil.OBJECT_MAPPER;
		int connectTimeout = Math.min(AsyncHttpClientConfigDefaults.defaultConnectTimeout(), 5000);
		int requestTimeout = Math.min(AsyncHttpClientConfigDefaults.defaultRequestTimeout(), 10000);
		int maxRequestRetry = 0;
		int maxRedirects = 0;
		int connectionTtl = 500;
		int pooledConnectionIdleTimeout = 1000;
		DefaultAsyncHttpClientConfig config = new DefaultAsyncHttpClientConfig.Builder()
			.setConnectTimeout(connectTimeout)
			.setRequestTimeout(requestTimeout)
			.setConnectionTtl(connectionTtl)
			.setMaxRequestRetry(maxRequestRetry)
			.setMaxRedirects(maxRedirects)
			.setPooledConnectionIdleTimeout(pooledConnectionIdleTimeout)
			.build();
		this.httpClient = new DefaultAsyncHttpClient(config);
		ownClient = true;
		backgroundThread = createBackgroundThread(null);
		backgroundThread.start();
	}
	
	private static final ThreadFactory DEFAULT_THREAD_FACTORY = new ThreadFactory() {
		
		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r);
			t.setName("ScraperClientThread");
			t.setDaemon(true);
			return t;
		}
	};
	
	/**
	 * Get the background thread that manages the work queue. This method is typically not useful.
	 * @return
	 */
	public Thread getBackgroundThread() {
		return backgroundThread;
	}
	
	/**
	 * Get the HTTP client that manages the actual request dispatch.
	 * @return
	 */
	public AsyncHttpClient getHttpClient() {
		return httpClient;
	}
	
	/**
	 * The maximum number of running requests that can be executed at a given time.
	 * @return
	 */
	public int getMaxConcurrentRequests() {
		return maxConcurrentRequests;
	}
	
	/**
	 * The minimum time between two adjacent requests.
	 * @return
	 */
	public long getThrottleMillis() {
		return throttleMillis;
	}
	
	/**
	 * Close the client, which you should always do.
	 * Once closed, no more requests can be submitted.
	 * Existing requests may be cancelled if not already completed.
	 * The background thread will terminate shortly after this method returns.
	 * @throws UndeclaredThrowableException a wrapped IOException if the HTTP client is owned by this object and cannot be closed properly. 
	 */
	public synchronized void close() {
		if (!shutdown) {
			shutdown = true;
			backgroundThread.interrupt();
			if (ownClient) {
				try {
					httpClient.close();
				} catch (IOException e) {
					throw new UndeclaredThrowableException(e);
				}
			}
		}
	}
	public synchronized void ensureValidState() {
		if (shutdown) {
			throw new IllegalStateException("already closed");
		}
	}
	
	/**
	 * Submit an asynchronous HTTP request producing a byte array result.
	 * Returns a {@link CompletableFuture} that can be used to obtain the result directly, and can also be composed with other {@link CompletableFuture} or {@link CompletionStage}
	 * to produce an asynchronous work pipeline.
	 * <p>
	 * This method returns immediately, the actual work is queued for asynchronous execution outside of the current thread.
	 * Since there is a queue, you can submit many requests at once (hundreds/thousands) and this class will queue and throttle the requests
	 * so that this client and the server are not overloaded.
	 * <p>
	 * Once the HTTP response is successfully received, the response body is read and converted to a byte[] for the result.
	 * <p>
	 * The result is wrapped in a {@link ScraperResult} which in addition to storing the result also includes additional information like the 
	 * HTTP {@link Request} and {@link Response} objects, as well as some timestamps for your measurements.
	 * <p>
	 * If you no longer want the response, you can call the {@link CompletableFuture#cancel(boolean)} method to abort the computation. This may
	 * possibly stop the HTTP data transfer, or even prevent the request from being submitted to the server.
	 * <p>
	 * If there is a failure with processing the request or if it is cancelled, the {@link ScraperResult} will strictly not be available to you, 
	 * and instead you will receive an exception if you try to get the result via {@link CompletableFuture#get()}.
	 * @param request
	 * @return
	 */
	public CompletableFuture<ScraperResult<byte[]>> requestBytes(Request request){
		return requestCustom(request, (req, res) -> res.getResponseBodyAsBytes());
	}
	
	/**
	 * Submit an asynchronous HTTP request producing a String result.
	 * Returns a {@link CompletableFuture} that can be used to obtain the result directly, and can also be composed with other {@link CompletableFuture} or {@link CompletionStage}
	 * to produce an asynchronous work pipeline.
	 * <p>
	 * This method returns immediately, the actual work is queued for asynchronous execution outside of the current thread.
	 * Since there is a queue, you can submit many requests at once (hundreds/thousands) and this class will queue and throttle the requests
	 * so that this client and the server are not overloaded.
	 * <p>
	 * Once the HTTP response is successfully received, the response body is read and converted to a String for the result using an appropriate charset.
	 * <p>
	 * The result is wrapped in a {@link ScraperResult} which in addition to storing the result also includes additional information like the 
	 * HTTP {@link Request} and {@link Response} objects, as well as some timestamps for your measurements.
	 * <p>
	 * If you no longer want the response, you can call the {@link CompletableFuture#cancel(boolean)} method to abort the computation. This may
	 * possibly stop the HTTP data transfer, or even prevent the request from being submitted to the server.
	 * <p>
	 * If there is a failure with processing the request or if it is cancelled, the {@link ScraperResult} will strictly not be available to you, 
	 * and instead you will receive an exception if you try to get the result via {@link CompletableFuture#get()}.
	 * @param request
	 * @return
	 */
	public CompletableFuture<ScraperResult<String>> requestString(Request request){
		return requestCustom(request, (req, res) -> res.getResponseBody());
	}
	/**
	 * Submit an asynchronous HTTP request producing a String result.
	 * Returns a {@link CompletableFuture} that can be used to obtain the result directly, and can also be composed with other {@link CompletableFuture} or {@link CompletionStage}
	 * to produce an asynchronous work pipeline.
	 * <p>
	 * This method returns immediately, the actual work is queued for asynchronous execution outside of the current thread.
	 * Since there is a queue, you can submit many requests at once (hundreds/thousands) and this class will queue and throttle the requests
	 * so that this client and the server are not overloaded.
	 * <p>
	 * Once the HTTP response is successfully received, the response body is read and converted to a String for the result using the supplied charset.
	 * <p>
	 * The result is wrapped in a {@link ScraperResult} which in addition to storing the result also includes additional information like the 
	 * HTTP {@link Request} and {@link Response} objects, as well as some timestamps for your measurements.
	 * <p>
	 * If you no longer want the response, you can call the {@link CompletableFuture#cancel(boolean)} method to abort the computation. This may
	 * possibly stop the HTTP data transfer, or even prevent the request from being submitted to the server.
	 * <p>
	 * If there is a failure with processing the request or if it is cancelled, the {@link ScraperResult} will strictly not be available to you, 
	 * and instead you will receive an exception if you try to get the result via {@link CompletableFuture#get()}.
	 * @param request
	 * @return
	 */
	public CompletableFuture<ScraperResult<String>> requestString(Request request, Charset charset){
		return requestCustom(request, (req, res) -> res.getResponseBody(charset));
	}

	/**
	 * Submit an asynchronous HTTP request producing an object representing the JSON content.
	 * Returns a {@link CompletableFuture} that can be used to obtain the result directly, and can also be composed with other {@link CompletableFuture} or {@link CompletionStage}
	 * to produce an asynchronous work pipeline.
	 * <p>
	 * This method returns immediately, the actual work is queued for asynchronous execution outside of the current thread.
	 * Since there is a queue, you can submit many requests at once (hundreds/thousands) and this class will queue and throttle the requests
	 * so that this client and the server are not overloaded.
	 * <p>
	 * Once the HTTP response is successfully received, the response body is read and parsed as a object of the given data type, using a JSON library for object mapping.
	 * <p>
	 * The result is wrapped in a {@link ScraperResult} which in addition to storing the result also includes additional information like the 
	 * HTTP {@link Request} and {@link Response} objects, as well as some timestamps for your measurements.
	 * <p>
	 * If you no longer want the response, you can call the {@link CompletableFuture#cancel(boolean)} method to abort the computation. This may
	 * possibly stop the HTTP data transfer, or even prevent the request from being submitted to the server.
	 * <p>
	 * If there is a failure with processing the request or if it is cancelled, the {@link ScraperResult} will strictly not be available to you, 
	 * and instead you will receive an exception if you try to get the result via {@link CompletableFuture#get()}.
	 * @param request
	 * @return
	 */
	public <T> CompletableFuture<ScraperResult<T>> requestJson(Request request, Class<T> jsonClass){
		return requestCustom(request, (req, res) -> readJsonObjectFromResponse(res, jsonClass));
	}
	
	
	/**
	 * Submit an asynchronous HTTP request producing an object that is transformed from the JSON content.
	 * Returns a {@link CompletableFuture} that can be used to obtain the result directly, and can also be composed with other {@link CompletableFuture} or {@link CompletionStage}
	 * to produce an asynchronous work pipeline.
	 * <p>
	 * This method returns immediately, the actual work is queued for asynchronous execution outside of the current thread.
	 * Since there is a queue, you can submit many requests at once (hundreds/thousands) and this class will queue and throttle the requests
	 * so that this client and the server are not overloaded.
	 * <p>
	 * Once the HTTP response is successfully received, the response body is read and parsed as a object of the given json class, using a JSON library for object mapping.
	 * Then the JSON object is transformed into the intended response data type using the supplied user function.
	 * Take note that the original untransformed JSON object will not be available to you. If you want it, then just use {@link #requestJson(Request, Class)} or
	 * {@link #requestCustom(Request, Class, BiFunction)} and do your own processing. You may read JSON with {@link #readJsonObjectFromResponse(Response, Class)}.
	 * <p>
	 * The result is wrapped in a {@link ScraperResult} which in addition to storing the result also includes additional information like the 
	 * HTTP {@link Request} and {@link Response} objects, as well as some timestamps for your measurements.
	 * <p>
	 * If you no longer want the response, you can call the {@link CompletableFuture#cancel(boolean)} method to abort the computation. This may
	 * possibly stop the HTTP data transfer, or even prevent the request from being submitted to the server.
	 * <p>
	 * If there is a failure with processing the request or if it is cancelled, the {@link ScraperResult} will strictly not be available to you, 
	 * and instead you will receive an exception if you try to get the result via {@link CompletableFuture#get()}.
	 * @param request
	 * @return
	 */
	public <T,U> CompletableFuture<ScraperResult<T>> requestJsonThenApply(Request request, Class<U> jsonClass, Function<U,T> transformer){
		return requestCustom(request, (req, res) -> transformer.apply(readJsonObjectFromResponse(res, jsonClass)));
	}
	
	/**
	 * Submit an asynchronous HTTP request producing an object representing the JSON content.
	 * Returns a {@link CompletableFuture} that can be used to obtain the result directly, and can also be composed with other {@link CompletableFuture} or {@link CompletionStage}
	 * to produce an asynchronous work pipeline.
	 * <p>
	 * This method returns immediately, the actual work is queued for asynchronous execution outside of the current thread.
	 * Since there is a queue, you can submit many requests at once (hundreds/thousands) and this class will queue and throttle the requests
	 * so that this client and the server are not overloaded.
	 * <p>
	 * Once the HTTP response is successfully received, the response body is read and parsed as a object of the given data type, using a JSON library for object mapping.
	 * <p>
	 * The result is wrapped in a {@link ScraperResult} which in addition to storing the result also includes additional information like the 
	 * HTTP {@link Request} and {@link Response} objects, as well as some timestamps for your measurements.
	 * <p>
	 * If you no longer want the response, you can call the {@link CompletableFuture#cancel(boolean)} method to abort the computation. This may
	 * possibly stop the HTTP data transfer, or even prevent the request from being submitted to the server.
	 * <p>
	 * If there is a failure with processing the request or if it is cancelled, the {@link ScraperResult} will strictly not be available to you, 
	 * and instead you will receive an exception if you try to get the result via {@link CompletableFuture#get()}.
	 * @param request
	 * @return
	 */
	public <T> CompletableFuture<ScraperResult<T>> requestJson(Request request, TypeReference<T> typeReference){
		return requestCustom(request, (req, res) -> readJsonObjectFromResponse(res, typeReference));
	}
	
	
	/**
	 * Submit an asynchronous HTTP request producing an object that is transformed from the JSON content.
	 * Returns a {@link CompletableFuture} that can be used to obtain the result directly, and can also be composed with other {@link CompletableFuture} or {@link CompletionStage}
	 * to produce an asynchronous work pipeline.
	 * <p>
	 * This method returns immediately, the actual work is queued for asynchronous execution outside of the current thread.
	 * Since there is a queue, you can submit many requests at once (hundreds/thousands) and this class will queue and throttle the requests
	 * so that this client and the server are not overloaded.
	 * <p>
	 * Once the HTTP response is successfully received, the response body is read and parsed as a object of the given json class, using a JSON library for object mapping.
	 * Then the JSON object is transformed into the intended response data type using the supplied user function.
	 * Take note that the original untransformed JSON object will not be available to you. If you want it, then just use {@link #requestJson(Request, Class)} or
	 * {@link #requestCustom(Request, Class, BiFunction)} and do your own processing. You may read JSON with {@link #readJsonObjectFromResponse(Response, Class)}.
	 * <p>
	 * The result is wrapped in a {@link ScraperResult} which in addition to storing the result also includes additional information like the 
	 * HTTP {@link Request} and {@link Response} objects, as well as some timestamps for your measurements.
	 * <p>
	 * If you no longer want the response, you can call the {@link CompletableFuture#cancel(boolean)} method to abort the computation. This may
	 * possibly stop the HTTP data transfer, or even prevent the request from being submitted to the server.
	 * <p>
	 * If there is a failure with processing the request or if it is cancelled, the {@link ScraperResult} will strictly not be available to you, 
	 * and instead you will receive an exception if you try to get the result via {@link CompletableFuture#get()}.
	 * @param request
	 * @return
	 */
	public <T,U> CompletableFuture<ScraperResult<T>> requestJsonThenApply(Request request, TypeReference<U> typeReference, Function<U,T> transformer){
		return requestCustom(request, (req, res) -> transformer.apply(readJsonObjectFromResponse(res, typeReference)));
	}
	
	
	
	/**
	 * Submit an asynchronous HTTP request producing a custom object.
	 * Returns a {@link CompletableFuture} that can be used to obtain the result directly, and can also be composed with other {@link CompletableFuture} or {@link CompletionStage}
	 * to produce an asynchronous work pipeline.
	 * <p>
	 * This method returns immediately, the actual work is queued for asynchronous execution outside of the current thread.
	 * Since there is a queue, you can submit many requests at once (hundreds/thousands) and this class will queue and throttle the requests
	 * so that this client and the server are not overloaded.
	 * <p>
	 * Once the HTTP response is successfully received, the supplied transformation function is called to produce a custom object as defined by the user.
	 * <p>
	 * The result is wrapped in a {@link ScraperResult} which in addition to storing the result also includes additional information like the 
	 * HTTP {@link Request} and {@link Response} objects, as well as some timestamps for your measurements.
	 * <p>
	 * If you no longer want the response, you can call the {@link CompletableFuture#cancel(boolean)} method to abort the computation. This may
	 * possibly stop the HTTP data transfer, or even prevent the request from being submitted to the server.
	 * <p>
	 * If there is a failure with processing the request or if it is cancelled, the {@link ScraperResult} will strictly not be available to you, 
	 * and instead you will receive an exception if you try to get the result via {@link CompletableFuture#get()}.
	 * @param request
	 * @return
	 */
	public <T> CompletableFuture<ScraperResult<T>> requestCustom(final Request request, final BiFunction<Request, Response, T> transformer){
		ScraperRequest<T> sreq = new ScraperRequest<T>(request, transformer);
		synchronized(lock) {
			ensureValidState();
			queue.add(sreq);
			lock.notifyAll(); //Wake up the background thread if it is currently waiting on the lock.
		}
		return sreq.getOuterFuture();
	}
	
	
	/**
	 * Reads the response body, parses it as JSON, and converts it to the intended object.
	 * <p>
	 * This method wraps JSON parsing exceptions into an {@link UndeclaredThrowableException} so that
	 * it can be called easily from lambda functions.
	 * <p>
	 * This method is intended to be used for the {@link #requestJsonThenApply(Request, Class, Class, Function)} and {@link #requestCustom(Request, Class, BiFunction)}
	 * methods.
	 * @param response
	 * @param typeReference
	 * @return
	 * @throws UndeclaredThrowableException wraps any checked {@link IOException} from the JSON parsing.
	 */
	public <T> T readJsonObjectFromResponse(Response response, TypeReference<T> typeReference) {
		try {
			return objectMapper.readValue(response.getResponseBodyAsBytes(), typeReference);
		} catch (IOException e) {
			throw new UndeclaredThrowableException(e);
		}
	}
	/**
	 * Reads the response body, parses it as JSON, and converts it to the intended object.
	 * <p>
	 * This method wraps JSON parsing exceptions into an {@link UndeclaredThrowableException} so that
	 * it can be called easily from lambda functions.
	 * <p>
	 * This method is intended to be used for the {@link #requestJsonThenApply(Request, Class, Class, Function)} and {@link #requestCustom(Request, Class, BiFunction)}
	 * methods.
	 * @param response
	 * @param jsonClass
	 * @return
	 * @throws UndeclaredThrowableException wraps any checked {@link IOException} from the JSON parsing.
	 */
	public <T> T readJsonObjectFromResponse(Response response, Class<T> jsonClass) {
		try {
			return objectMapper.readValue(response.getResponseBodyAsBytes(), jsonClass);
		} catch (IOException e) {
			throw new UndeclaredThrowableException(e);
		}
	}
	
	private final synchronized Thread createBackgroundThread(ThreadFactory factory) {
		factory = factory == null ? DEFAULT_THREAD_FACTORY : factory;
		final Thread t = factory.newThread(new Runnable() {
			@Override
			public void run() {
				final Semaphore semaphore = new Semaphore(maxConcurrentRequests);
				final long throttleNanos = throttleMillis * 1_000_000;
				long nextNanos = 0;
				boolean acquiredPermit = false;
				while (!shutdown) {
					ScraperRequest<?> req = null;
					synchronized(lock) {
						while(!shutdown && queue.isEmpty()) {
							try {
								lock.wait(1800_000); //Wait 30 minutes.
							} catch (InterruptedException e) {
								
								//If interrupted, try again next loop... Unless queue is not empty.
							}
						}
						if (shutdown) {
							break;
						} else {
							//Queue must be non-empty.
							while (true) {
								req = queue.pollFirst();
								if (req == null) {
									break;
								} else if (req.getStatus() != Status.QUEUING) {
									//This request is not queuing, don't bother.
									req = null;
									continue;
								} else if (req.getOuterFuture().isDone()) {
									//This request is already done, don't bother.
									req = null;
									continue;
								} else {
									//No problem with this one.
									break;
								}
							}
						}
					}
					//Now outside the synchronized block, let's peek into our request, if it exists.
					if (req != null) {
						if (req.getOuterFuture().isDone()) {
							//Bleh skip.
							continue;
						}
						//Do we need to be throttled before servicing this request?
						long now = System.nanoTime();
						while (!shutdown && now < nextNanos) {
							long pauseNanos = Math.min(200_000, Math.max(1_000_000_000L, nextNanos - now));
							LockSupport.parkNanos(pauseNanos);
							now = System.nanoTime();
						}
						if (shutdown) {
							req.cancel();
							break;
						}
						//Okay, we do not need to be throttled.
						//What about semaphore?
						while (!acquiredPermit && !shutdown) {
							try {
								if (semaphore.tryAcquire(1_000_000_000L, TimeUnit.NANOSECONDS)) {
									acquiredPermit = true;
									break;
								}
							} catch (InterruptedException e) {
								//If interrupted try again.
							}
						}
						//We got semaphore. What if we encountered an error?
						
						
						if (shutdown) {
							if (acquiredPermit) {
								semaphore.release(); //Not like there's any point to release at this stage.
								acquiredPermit = false;
							}
							req.cancel();
							break;
						}
						//We are ready to submit, just do another check on the status of the request.
						if (req.getOuterFuture().isDone()) {
							//Release the permit.
							semaphore.release();
							acquiredPermit = false; 
							continue;
						} else {
							//send to client.
							
							CompletableFuture<Response> innerFuture;
							synchronized(lock) {
								if (shutdown) {
									semaphore.release();
									acquiredPermit = false; 
									req.cancel();
									break;
								}
								final ScraperRequest<?> req2 = req;
								nextNanos = System.nanoTime() + throttleNanos;
								acquiredPermit = false;
								innerFuture = httpClient.executeRequest(req.getRequest(), new AsyncCompletionHandlerBase() {
									public AsyncHandler.State onBodyPartReceived(HttpResponseBodyPart content) throws Exception {
										return abortIfNeeded(super.onBodyPartReceived(content));
									}
									public AsyncHandler.State onHeadersReceived(HttpHeaders headers) throws Exception {
										return abortIfNeeded(super.onHeadersReceived(headers));
									}
									public AsyncHandler.State onContentWriteProgress(long amount, long current, long total) {
										return abortIfNeeded(super.onContentWriteProgress(amount, current, total));
									}
									public AsyncHandler.State onContentWritten() {
										return abortIfNeeded(super.onContentWritten());
									}
									public AsyncHandler.State onHeadersWritten() {
										return abortIfNeeded(super.onHeadersWritten());
									}
									public AsyncHandler.State onStatusReceived(HttpResponseStatus status) throws Exception {
										return abortIfNeeded(super.onStatusReceived(status));
									}
									public AsyncHandler.State onTrailingHeadersReceived(HttpHeaders headers) throws Exception {
										return abortIfNeeded(super.onTrailingHeadersReceived(headers));
									}
									public AsyncHandler.State abortIfNeeded(AsyncHandler.State superState) {
										if (superState == AsyncHandler.State.ABORT) {
											return superState;
										} else if (shutdown || req2.getOuterFuture().isDone() || req2.innerFutureAbort) {
											return AsyncHandler.State.ABORT;
										} else {
											return AsyncHandler.State.CONTINUE;
										}
									}
								}).toCompletableFuture();
								innerFuture.whenComplete(new BiConsumer<Response, Throwable>(){
									@Override
									public void accept(Response t, Throwable u) {
										semaphore.release(); // Release the permit.
									}
								});
							}
							req.advance(innerFuture);
						}
						
					}
				}
				shutdown();
				// TODO Auto-generated method stub
				
			}
			
			public void shutdown() {
				List<ScraperRequest<?>> reqs;
				synchronized(lock) {
					reqs = new ArrayList<>(queue);
					queue.clear();
				}
				for (ScraperRequest<?> req : reqs) {
					req.cancel();
				}
			}
		});
		return t;
	}
	
}
