package astar.ihpc.umgc.scraper.util;

import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.codec.digest.DigestUtils;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
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
	public static final ZoneId STANDARD_ZONE = ZoneId.of("Asia/Singapore");

	/**
	 * A standard formatter. DateTimeFormatter is the new Java 8 formatter class, and is incompatible with the older DateFormat class.
	 */
	public static final DateTimeFormatter STANDARD_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy.MM.dd.HH.mm.ss").withZone(STANDARD_ZONE);
	/**
	 * A standard format. DateFormat is the older Java formatter class, and is incompatible with the new Java 8 date time API.
	 */
	public static final DateFormat STANDARD_DATE_FORMAT = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
	static {
		STANDARD_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone(STANDARD_ZONE));
	}
	
	
	/**
	 * An object mapper with sensible default configurations. AS THIS INSTANCE IS SHARED GLOBALLY, DO NOT CHANGE ITS CONFIGURATION.
	 */
	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	static {
		OBJECT_MAPPER.setDateFormat(STANDARD_DATE_FORMAT);
	}
	
	/**
	 * A secure random number generator. Thread-safe and can be shared. Sequences are inherently unpredictable.
	 */
	public static final SecureRandom SECURE_RANDOM = new SecureRandom();
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
	 * Convert a local date time to milliseconds since epoch. The Singapore time zone is assumed.
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
		return LocalDateTime.of(year, month, dayOfMonth, hour, minute, second, 0).atZone(STANDARD_ZONE).toInstant().toEpochMilli();
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
	
	private static char[] NONCE_CHARS = {'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','0','1','2','3','4','5','6','7','8','9'};
	private static boolean[] STANDARD_CHARS_LOOKUP = new boolean[256];
	static {
		for (int i = 0; i < 26; i++) {
			STANDARD_CHARS_LOOKUP[((int)('a')) + i] = true;
			STANDARD_CHARS_LOOKUP[((int)('A')) + i] = true;
		}
		for (int i = 0; i < 10; i++) {
			STANDARD_CHARS_LOOKUP[((int)('0')) + i] = true;
		}
		STANDARD_CHARS_LOOKUP[(int)'-'] = true;
		STANDARD_CHARS_LOOKUP[(int)'_'] = true;
	}
	
	
	
		
			
	/**
	 * Create a random 8 character alphabetical string that can be used to avoid collisions when generating filenames. 
	 * <p>
	 * The allowed characters are the 26 lowercase alphabets and 10 decimal digit characters. (Essentially base-36.) Uppercase is not allowed as Windows treats filenames as case-insensitive.
	 * @return
	 */
	public static String calculateShortNonce() {
		char[] out = new char[8];
		for (int i = 0; i < out.length; i++) {
			out[i] = NONCE_CHARS[SECURE_RANDOM.nextInt(NONCE_CHARS.length)];
		}
		return new String(out);
	}
	
	/**
	 * Create a random 16 character alphabetical string that can be used to avoid collisions when generating filenames. 
	 * <p>
	 * The allowed characters are the 26 lowercase alphabets and 10 decimal digit characters. (Essentially base-36.) Uppercase is not allowed as Windows treats filenames as case-insensitive.
	 * @return
	 */
	public static String calculateMediumNonce() {
		char[] out = new char[16];
		for (int i = 0; i < out.length; i++) {
			out[i] = NONCE_CHARS[SECURE_RANDOM.nextInt(NONCE_CHARS.length)];
		}
		return new String(out);
	}
	
	/**
	 * Create a random 24 character alphabetical string that can be used to avoid collisions when generating filenames. 
	 * <p>
	 * The allowed characters are the 26 lowercase alphabets and 10 decimal digit characters. (Essentially base-36.) Uppercase is not allowed as Windows treats filenames as case-insensitive.
	 * @return
	 */
	public static String calculateLongNonce() {
		char[] out = new char[24];
		for (int i = 0; i < out.length; i++) {
			out[i] = NONCE_CHARS[SECURE_RANDOM.nextInt(NONCE_CHARS.length)];
		}
		return new String(out);
	}
	
	private static void ensureStandardChars(String s, boolean acceptEmpty) {
		try {
			if (!acceptEmpty && s.length() == 0) {
				throw new IllegalArgumentException("empty string is not allowed");
			}
			for (int i = 0; i < s.length(); i++) {
				char c = s.charAt(i);
				if (!STANDARD_CHARS_LOOKUP[c]) {
					throw new IllegalArgumentException("Invalid characters in " + s);
				}
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new IllegalArgumentException("Invalid characters in " + s);
		}
	}
	/**
	 * Generate a directory path appropriate for storing scraped data according to the specified parameters.
	 * <p>
	 * This can be combined with {@link #createStandardScraperFileName(String, String, long, String...)} to build a full filepath.
	 * <p>
	 * This method will standardise all our scrapers to this naming convention:
	 * "/umgc/data/scraper/{zone}/{scraper}/{dataset}/{YYYY}/{MM}/{DD}/" (including trailing slash)
	 * <p>
	 * Each day is in its separate folder to ensure that directory contents are not too large. (Cannot have thousands of entries in one dir.)
	 * <p>
	 * Dates are formatted according to Singapore time. (Important if you're running a cloud scraper and its local timezone is not set properly to Singapore time.)
	 * <p>
	 * timeMillis is the time corresponding to the request. For periodic datasets fetched using RealTimeStepper, this should be the return value of {@link RealTimeStepper#nextStep()}.
	 * For ad-hoc datasets not using RealTimeStepper, this can be the current time {@link System#currentTimeMillis()}.
	 * <p>
	 * All characters must belong to this range: a-Z, 0-9, hyphen, underscore. No other characters allowed. No spaces allowed.
	 * <p>
	 * Hyphen is preferred over underscore (the underscore is losing popularity in URLs).
	 * @param zone identifier for the network zone running the scraper
	 * @param scraperId identifier for the scraper. must be globally unique
	 * @param dataset the name of the dataset
	 * @param timeMillis the time of the request (only the date component is extracted)
	 * @return
	 */
	public static String calculateStandardScraperDirName(String zone, String scraperId, String dataset, long timeMillis) {
		ensureStandardChars(zone, false);
		ensureStandardChars(scraperId, false);
		ensureStandardChars(dataset, false);
		OffsetDateTime dt = OffsetDateTime.ofInstant(Instant.ofEpochMilli(timeMillis), STANDARD_ZONE);
		int year = dt.getYear();
		int month = dt.getMonthValue();
		int dayOfMonth = dt.getDayOfMonth();
		return String.format("/umgc/data/scraper/%s/%s/%s/%04d/%02d/%02d/", zone, scraperId, dataset, year, month, dayOfMonth);
	}
	
	/**
	 * Generate a standard filename appropriate for storing scraped data according to the specified parameters.
	 * <p>
	 * This can be combined with {@link #calculateStandardScraperDirName(String, String, String, long)} to build a full filepath.
	 * <p>
	 * This method will standardise the filenames to this convention:
	 * "YYYY.MM.DD.HH.MM.SS.SUFFIX1.SUFFIX2.EXTENSION"
	 * It is assumed that datasets are not written faster than second precision, thus, milliseconds are not shown.
	 * <p>
	 * Dates are formatted according to Singapore time. (Important if you're running a cloud scraper and its local timezone is not set properly to Singapore time.)
	 * <p>
	 * timeMillis is the time corresponding to the request. For periodic datasets fetched using RealTimeStepper, this should be the return value of {@link RealTimeStepper#nextStep()}.
	 * For ad-hoc datasets not using RealTimeStepper, this can be the current time {@link System#currentTimeMillis()}.
	 * <p>
	 * The period must not be included in the extension (it will be added automatically).
	 * <p>
	 * Suffixes are additional dataset-specific suffixes. If null or not included, a suffix will not be included.
	 * <p>
	 * All characters must belong to this range: a-Z, 0-9, hyphen, underscore. No other characters allowed. No spaces allowed.
	 * <p>
	 * Hyphen is preferred over underscore (the underscore is losing popularity in URLs).
	 * @param extension the file extension (without period)
	 * @param timeMillis the time of the request
	 * @param suffixes optional suffixes to be generated.
	 * @return
	 */
	public static String calculateStandardScraperFileName(String extension, long timeMillis, String... suffixes) {
		ensureStandardChars(extension, false);
		if (suffixes == null) suffixes = new String[0];
		for (String s : suffixes) {
			ensureStandardChars(s, false);
		}
		OffsetDateTime dt = OffsetDateTime.ofInstant(Instant.ofEpochMilli(timeMillis), STANDARD_ZONE);
		int year = dt.getYear();
		int month = dt.getMonthValue();
		int dayOfMonth = dt.getDayOfMonth();
		int hr = dt.getHour();
		int min = dt.getMinute();
		int sec = dt.getSecond();
		String yrMthDayStr = String.format("%04d.%02d.%02d.%02d.%02d.%02d", year,month,dayOfMonth,hr,min,sec);
		StringBuilder sb = new StringBuilder();
		for (String s : suffixes) {
			sb.append(".").append(s);
		}
		return yrMthDayStr + sb.toString() + "." + extension;
	}
	
	
	
	public static String calculateHash(byte[] data) {
		return DigestUtils.sha256Hex(data);
	}
	public static String calculateHash(InputStream data) throws IOException {
		return DigestUtils.sha256Hex(data);
	}
	public static String calculateHash(String data) {
		return DigestUtils.sha256Hex(data);
	}
	
	public static String writeJsonToString(Object value) throws JsonProcessingException{
		return OBJECT_MAPPER.writeValueAsString(value);
	}
	
	public static String calculateFileNameFromFilePath(String filePath) {
		int ix = filePath.lastIndexOf('/');
		if (ix == -1) return filePath;
		return filePath.substring(ix+1);
	}
	
	public static String calculateDirNameFromFilePath(String filePath) {
		int ix = filePath.lastIndexOf('/');
		if (ix == -1) return "/";
		return filePath.substring(0, ix+1);
	}
	
}
