package astar.ihpc.umgc.umgcscraper.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Helper methods to do web scraping.
 * <p>
 * The methods can be broadly categorised into two: (a) timing methods (for reliable polling of frequently updated data) and (b) HTTP request building and dispatching.
 * <p>
 * For ad-hoc reliable sleeping, call the {@link #sleepUntil(long)}, {@link #sleepFor(long)}, and related methods. These methods are quite robust against changes to the system clock or drifting.
 * <p>
 * For reliable real-time sleeping, call the {@link #createRealTimeStepper(long, long, long, long)} method. This stepper can be used to reliably wake up with a fixed
 * period. This is most useful for synchronising your scraper requests with a web server's dataset update schedule. To convert a real-world date (e.g., DD-MM-YYYY HH:MM:SS) to a timestamp for this
 * for use with the time stepper, use the {@link #convertToTimeMillis(Instant)} and related methods.
 * <p>
 * For submitting web scraping requests, call the {@link #createScraperClient(int, long, ObjectMapper, AsyncHttpClient)} or {@link #createScraperClient(int, long)} methods.
 * This client class can help you submit numerous requests asynchronously, and throttle the requests to prevent overloading the server. To build {@link Request} objects
 * to be used for the scraper client, use the 
 * 
 * 
 * 
 * @author othmannb
 *
 */
public final class ScraperUtil {
	private final static boolean HI_RES_SLEEP_ENABLED = initHiResSleep();
	private ScraperUtil() {
		
	}
	
	private final static boolean initHiResSleep() {
		Thread hiresSleepThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				while (true) {
					try {
						Thread.sleep(Long.MAX_VALUE);
					} catch (InterruptedException e) {
						return;
					}
				}
			}
		});
		hiresSleepThread.setName("Hi-res sleep thread");
		hiresSleepThread.setDaemon(true);
		hiresSleepThread.start();
		return hiresSleepThread.isAlive();
	}
	
	/**
	 * Workaround for Windows to enable high-resolution sleep timer. Not needed for other OS.
	 * @return
	 */
	public final static boolean enableHiResSleep() {
		return HI_RES_SLEEP_ENABLED;
	}

	
	/**
	 * Sleep the current thread reliably until the deadline time is passed. The deadline time is given as milliseconds since epoch.
	 * @param deadlineMillis
	 */
	public final static void sleepUntil(long deadlineMillis) {
		long now = System.currentTimeMillis();
		while (now < deadlineMillis) {
			long diff = deadlineMillis - now;
			if (diff == 1) {
				LockSupport.parkNanos(350_000L); //0.35 milliseconds
			} else {
				LockSupport.parkNanos(Math.min(30_000_000_000L, diff * 1_000_000L)); //Sleep for 30 seconds at a time.
			}
			now = System.currentTimeMillis();
			Thread.interrupted(); //Clear interrupted flag if any (ignore interruptions).
		}
	}
	
	/**
	 * Sleep the current thread reliably until the deadline time is passed or the thread is interrupted. The deadline time is given as milliseconds since epoch.
	 * @param deadlineMillis
	 */
	public final static void sleepUntilInterruptibly(long deadlineMillis) throws InterruptedException {
		long now = System.currentTimeMillis();
		while (now < deadlineMillis) {
			long diff = deadlineMillis - now;
			if (diff == 1) {
				LockSupport.parkNanos(350_000L); //0.35 milliseconds
			} else {
				LockSupport.parkNanos(Math.min(30_000_000_000L, diff * 1_000_000L)); //Sleep for 30 seconds at a time.
			}
			now = System.currentTimeMillis();
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
		}
	}
	
	
	/**
	 * Sleep the current thread reliably for the given duration in milliseconds.
	 * @param sleepMillis
	 */
	public final static void sleepFor(long sleepMillis) {
		long nanos = System.nanoTime();
		long wakeup = nanos + sleepMillis * 1_000_000L;
		while (nanos < wakeup) {
			LockSupport.parkNanos(wakeup - nanos);
			nanos = System.nanoTime();
			Thread.interrupted(); //Clear interrupted flag if any (ignore interruptions).
		}
	}
	
	/**
	 * Sleep the current thread reliably for the given duration in milliseconds or until the thread is interrupted.
	 * @param sleepMillis
	 * @throws InterruptedException
	 */
	public final static void sleepForInterruptibly(long sleepMillis) throws InterruptedException {
		long nanos = System.nanoTime();
		long wakeup = nanos + sleepMillis * 1_000_000L;
		while (nanos < wakeup) {
			LockSupport.parkNanos(wakeup - nanos);
			nanos = System.nanoTime();
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
		}
	}
	
	/**
	 * Sleep the current thread reliably for the given duration in nanoseconds.
	 * @param sleepNanos
	 */
	public final static void sleepForNanos(long sleepNanos) {
		long nanos = System.nanoTime();
		long wakeup = nanos + sleepNanos;
		while (nanos < wakeup) {
			LockSupport.parkNanos(wakeup - nanos);
			nanos = System.nanoTime();
			Thread.interrupted(); //Clear interrupted flag if any (ignore interruptions).
		}
	}
	
	/**
	 * Sleep the current thread reliably for the given duration in nanoseconds or until the thread is interrupted.
	 * @param sleepNanos
	 * @throws InterruptedException
	 */
	public final static void sleepForNanosInterruptibly(long sleepNanos) throws InterruptedException {
		long nanos = System.nanoTime();
		long wakeup = nanos + sleepNanos;
		while (nanos < wakeup) {
			LockSupport.parkNanos(wakeup - nanos);
			nanos = System.nanoTime();
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
		}
	}
	
	/**
	 * Schedules a delay on the given {@link ScheduledThreadPoolExecutor}, returning a CompletableFuture.
	 * The CompletableFuture allows you to compose the delay into a pipeline.
	 * <p>Warning: Do not shutdown the scheduler until the future is done (either completed or cancelled). Otherwise
	 * any thread waiting on the future will hang forever.
	 * @param scheduler
	 * @param delayMillis
	 * @return
	 */
	public static CompletableFuture<Void> scheduleDelay(ScheduledThreadPoolExecutor scheduler, long delayMillis){
		final CompletableFuture<Void> out = new CompletableFuture<Void>() {
			final CompletableFuture<Void> me = this;
			final ScheduledFuture<?> scheduledFuture = scheduler.schedule(()->{
				me.complete(null);
			}, delayMillis, TimeUnit.MILLISECONDS);
			
			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				boolean out = super.cancel(mayInterruptIfRunning);
				scheduledFuture.cancel(true);
				return out;
			}
			@Override
			public boolean completeExceptionally(Throwable ex) {
				boolean out = super.completeExceptionally(ex);
				scheduledFuture.cancel(true);
				return out;
			}
			@Override
			public boolean complete(Void value) {
				boolean out = super.complete(value);
				if (out) {
					scheduledFuture.cancel(true);
				}
				return out;
			}
		};
		return out;
	}
	
	/**
	 * Convert a local date time to milliseconds since epoch. The system default time zone is assumed.
	 * Useful to generate the startTimeMillis parameter in {@link RealTimeStepper}.
	 * @param dateTime
	 * @return
	 */
	public static long convertToTimeMillis(LocalDateTime dateTime) {
		return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
	}
	
	/**
	 * Convert a zoned date time to milliseconds since epoch.
	 * Useful to generate the startTimeMillis parameter in {@link RealTimeStepper}.
	 * @param dateTime
	 * @return
	 */
	public static long convertToTimeMillis(OffsetDateTime dateTime) {
		return dateTime.toInstant().toEpochMilli();
	}
	
	/**
	 * Convert an offset date time to milliseconds since epoch.
	 * Useful to generate the startTimeMillis parameter in {@link RealTimeStepper}.
	 * @param dateTime
	 * @return
	 */
	public static long convertToTimeMillis(ZonedDateTime dateTime) {
		return dateTime.toInstant().toEpochMilli();
	}
	
	/**
	 * Convert a date-time Instant to milliseconds since epoch.
	 * Useful to generate the startTimeMillis parameter in {@link RealTimeStepper}.
	 * @param instant
	 * @return
	 */
	public static long convertToTimeMillis(Instant instant) {
		return instant.toEpochMilli();
	}
	
	/**
	 * Convert a local date time to milliseconds since epoch. The system default time zone is assumed.
	 * Useful to generate the startTimeMillis parameter in {@link RealTimeStepper}.
	 * @param year
	 * @param month
	 * @param dayOfMonth
	 * @param hour
	 * @param minute
	 * @param second
	 * @return
	 */
	public static long convertToTimeMillis(int year, int month, int dayOfMonth, int hour, int minute, int second) {
		return LocalDateTime.of(year, month, dayOfMonth, hour, minute, second, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
	}
	
	/**
	 * Convert a zoned date time to milliseconds since epoch.
	 * Useful to generate the startTimeMillis parameter in {@link RealTimeStepper}.
	 * @param year
	 * @param month
	 * @param dayOfMonth
	 * @param hour
	 * @param minute
	 * @param second
	 * @param zone
	 * @return
	 */
	public static long convertToTimeMillis(int year, int month, int dayOfMonth, int hour, int minute, int second, ZoneId zone) {
		return ZonedDateTime.of(year, month, dayOfMonth, hour, minute, second, 0, zone).toInstant().toEpochMilli();
	}
	
	/**
	 * Convert an offset date time to milliseconds since epoch.
	 * Useful to generate the startTimeMillis parameter in {@link RealTimeStepper}.
	 * @param year
	 * @param month
	 * @param dayOfMonth
	 * @param hour
	 * @param minute
	 * @param second
	 * @param offset
	 * @return
	 */
	public static long convertToTimeMillis(int year, int month, int dayOfMonth, int hour, int minute, int second, ZoneOffset offset) {
		return ZonedDateTime.of(year, month, dayOfMonth, hour, minute, second, 0, offset).toInstant().toEpochMilli();
	}
	
	/**
	 * Constructs a new RealTimeStepper. It is a helper to sleep the current thread and wake up based on a realtime schedule.
	 * <p>
	 * This method is identical to calling the {@link RealTimeStepper#RealTimeStepper(long, long, long, long)} constructor manually.
	 * It is in this class for your convenience.
	 * @param startTimeMillis the time of the first tick, as milliseconds since Unix Epoch (1 Jan 1970 UTC).
	 * @param timeStepMillis the time step in milliseconds
	 * @param maxOvershootMillis the maximum allowed delay of a time step, before the step is skipped
	 * @param maxRandomDelayMillis the maximum random delay to add to a time step, useful to avoid multiple identical TimeSteppers (in different programs) stepping at exactly same time
	 */
	public static RealTimeStepper createRealTimeStepper(long startTimeMillis, long timeStepMillis, long maxOvershootMillis, long maxRandomDelayMillis) {
		return new RealTimeStepper(startTimeMillis, timeStepMillis, maxOvershootMillis, maxRandomDelayMillis);
	}

	/**
	 * Create a ScraperClient with supplied dependencies.
	 * Use this method if you want to change the behaviour of the JSON parser or the underlying HTTP client.
	 * <p>
	 * The ScraperClient allows you to queue numerous requests at once, and it will process the requests asynchronously in a throttled manner
	 * to avoid overloading the server. It also has several methods to help you transform the HTTP response into something more usable.
	 * <p>
	 * This method is identical to calling the {@link ScraperClient#ScraperClient(int, long, ObjectMapper, AsyncHttpClient)} constructor manually.
	 * It is in this class for your convenience.
	 * <p>
	 * Warning: Closing this client will NOT close() the underlying HTTP client (available from {@link #getHttpClient()}) since the client is provided
	 * by the user, and therefore the user should close the client themselves.
	 * @param maxConcurrentRequests the maximum number of running requests that can be executed at a given time (recommended 4-16)
	 * @param throttleMillis the minimum time between two adjacent requests (recommended 50)
	 * @param objectMapper used to parse JSON into Java objects (from Jackson library)
	 * @param httpClient the asynchronous HTTP client that will do the actual dispatching (from async-http-client library)
	 */
	public static ScraperClient createScraperClient(int maxConcurrentRequests, int throttleMillis, ObjectMapper objectMapper, AsyncHttpClient httpClient) {
		return new ScraperClient(maxConcurrentRequests, throttleMillis, objectMapper, httpClient);
	}
	/**
	 * Create a ScraperClient with default dependencies.
	 * The ObjectMapper will be a plain new ObjectMapper() and the AsyncHttpClient will be a new DefaultAsyncHttpClient().
	 * <p>
	 * The ScraperClient allows you to queue numerous requests at once, and it will process the requests asynchronously in a throttled manner
	 * to avoid overloading the server. It also has several methods to help you transform the HTTP response into something more usable.
	 * <p>
	 * This method is identical to calling the {@link ScraperClient#ScraperClient(int, long)} constructor manually.
	 * It is in this class for your convenience.
	 * <p>
	 * Warning: Closing this client will also close() the underlying HTTP client (available from {@link #getHttpClient()}) since the client is owned
	 * and managed by this class.
	 * @param maxConcurrentRequests the maximum number of running requests that can be executed at a given time (recommended 4-16)
	 * @param throttleMillis the minimum time between two adjacent requests (recommended 50)
	 * @param objectMapper used to parse JSON into Java objects (from Jackson library)
	 * @param httpClient the asynchronous HTTP client that will do the actual dispatching (from async-http-client library)
	 */
	public static ScraperClient createScraperClient(int maxConcurrentRequests, int throttleMillis) {
		return new ScraperClient(maxConcurrentRequests, throttleMillis);
	}
	
	/**
	 * Create a {@link RequestBuilder} that you can use to build a {@link Request}. The {@link RequestBuilder} has a fluent API for you to
	 * specify the properties of your request easily. Once done, call the {@link RequestBuilder#build()} method to build the {@link Request}.
	 * <p>
	 * A single {@link RequestBuilder} can only be used to create one {@link Request} object. For other kinds of Requests, you need to create a new builder.
	 * <p>
	 * However, you can recycle the same {@link Request} instance as input to the ScraperClient. That means you are going to request the same resource
	 * repeatedly from the server.
	 * @return
	 */
	public static RequestBuilder createRequestBuilder() {
		return new RequestBuilder();
	}
	
	
}
