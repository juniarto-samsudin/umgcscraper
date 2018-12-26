package astar.ihpc.umgc.scraper.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;

import org.asynchttpclient.Request;

/**
 * A PaginationRequest that encapsulates a multiple-page request.
 * This is a stateless object that can be shared between multiple threads.
 * @author othmannb
 *
 * @param <T>
 */
public class PaginationRequest<T> {
	private final IntFunction<Request> pageCreateFunction;
	private final Function<Request, CompletableFuture<ScraperResult<T>>> pageRequestFunction;
	private final ScheduledThreadPoolExecutor scheduler;
	private final int batchSize;
	private final Predicate<ScraperResult<T>> lastPageTest;
	private final Predicate<ScraperResult<T>> emptyPageTest;
	private final Predicate<ScraperResult<T>> goodResultTest;
	private final BiPredicate<Request, Throwable> retryOnErrorTest;
	private final int maxRetries;
	private final int retryMinDelayMillis, retryMaxDelayMillis;
	private final AtomicLong previouslyKnownLastPage = new AtomicLong(10000);
	public PaginationRequest(IntFunction<Request> pageCreateFunction, 
			Function<Request, CompletableFuture<ScraperResult<T>>> pageRequestFunction, ScheduledThreadPoolExecutor scheduler, int batchSize, 
			Predicate<ScraperResult<T>> lastPageTest, Predicate<ScraperResult<T>> emptyPageTest, Predicate<ScraperResult<T>> goodResultTest,
			BiPredicate<Request, Throwable> retryOnErrorTest, int maxRetries, int retryMinDelayMillis, int retryMaxDelayMillis) {
		super();
		this.pageCreateFunction = pageCreateFunction;
		this.pageRequestFunction = pageRequestFunction;
		this.scheduler = scheduler;
		this.batchSize = batchSize;
		this.emptyPageTest = emptyPageTest;
		this.lastPageTest = lastPageTest;
		this.goodResultTest = goodResultTest;
		this.retryOnErrorTest = retryOnErrorTest;
		this.maxRetries = maxRetries;
		this.retryMinDelayMillis = retryMinDelayMillis;
		this.retryMaxDelayMillis = retryMaxDelayMillis;
	}
	public IntFunction<Request> getPageCreateFunction() {
		return pageCreateFunction;
	}
	public int getBatchSize() {
		return batchSize;
	}
	public Predicate<ScraperResult<T>> getLastPageTest() {
		return lastPageTest;
	}
	public Predicate<ScraperResult<T>> getEmptyPageTest() {
		return emptyPageTest;
	}
	public Predicate<ScraperResult<T>> getGoodResultTest() {
		return goodResultTest;
	}
	public BiPredicate<Request, Throwable> getRetryOnErrorTest() {
		return retryOnErrorTest;
	}
	public int getRetryMinDelayMillis() {
		return retryMinDelayMillis;
	}
	public int getRetryMaxDelayMillis() {
		return retryMaxDelayMillis;
	}
	public int getMaxRetries() {
		return maxRetries;
	}
	
	/**
	 * Create an object representing a request for the given page. This is a state-less object that encapsulates the parameters of the request.
	 * It does not actually submit the request.
	 * <p>
	 * This code is equivalent to pageCreateFunction.apply(pageNo).
	 * @param pageNo
	 * @return
	 */
	public Request createPage(int pageNo){
		return pageCreateFunction.apply(pageNo);
	}
	
	
	/*
	 * Create and dispatch multiple requests to fetch all pages.
	 * Up to {@link #getBatchSize()} requests are batched at one time, although the batch may be further throttled by the underlying HTTP client.
	 * <p>
	 * Returns a future comprising of a list of results for each successful page. If {@link #getEmptyPageTest()} is not null, then any results which are empty
	 * (as tested) will be omitted from the list.
	 * <p>
	 * This method will request iteratively more and more pages until it encounters a page which returns true on {@link #getLastPageTest()}. 
	 * Then it will halt the execution of the remaining pages, and prevent new pages from being submitted.
	 * <p>
	 * If {@link #getGoodResultTest()} is not null, it will be used to test all returned results. If the test returns false, then it means
	 * the result is bad, and it will be retried after #getRetryMinDelayMillis() to #getRetryMaxDelayMillis() seconds (up to #getMaxRetries() times).
	 * <p>
	 * If {@link #getRetryOnErrorTest()} is set, then any exceptions (except cancellations) that occur will be tested. If the test returns true, then
	 * the page is retried after #getRetryMinDelayMillis() to #getRetryMaxDelayMillis() seconds (up to #getMaxRetries() times).
	 * <p>
	 * If there is an unrecoverable error (either run out of retries or no retries allowed) on any single page, then the entire batch is failed.
	 * @param pageRequestFunction the function to dispatch the request
	 * @param deadlineMillis the maximum deadline of this function, after which the future will fail, in milliseconds from epoch. If negative or zero, the deadline is ignored. 
	 * @param scheduler a scheduler to schedule retries. If {@link #getMaxRetries()} is zero, then this is allowed to be null.
	 * @return a future that holds a list of all completed pages.
	 */
	private CompletableFuture<PaginationResult<T>> requestPages(final long deadlineMillis, List<Integer> initialPages, final boolean requestNextPages){
		final long createTimeMillis = System.currentTimeMillis();
		final long effectiveDeadlineMillis = deadlineMillis <= 0 ? Long.MAX_VALUE : deadlineMillis;
		final List<Integer> initialPages2 = new ArrayList<>(initialPages);
		for (int i = 0; i < initialPages2.size(); i++) {
			int v = initialPages2.get(i);
			if (v < 0) throw new IllegalArgumentException();
			if (i > 0) {
				int pv = initialPages2.get(i-1);
				if (pv >= v) throw new IllegalArgumentException();
			}
		}
		final Function<Request, CompletableFuture<ScraperResult<T>>> origPageRequestFunction = pageRequestFunction;
		final long previouslyKnownLastPageValue = this.previouslyKnownLastPage.get();
		class PaginationRequestWorker{
			final PaginationRequestWorker lock = this;
			final CompletableFuture<PaginationResult<T>> finalResult;
			final Map<Integer, CompletableFuture<ScraperResult<T>>> futures = new TreeMap<>();
			final Map<Integer, Request> cachedRequests = new LinkedHashMap<>();
			final Map<Integer, Integer> retryCounts = new LinkedHashMap<>();
			AtomicBoolean done = new AtomicBoolean(false);
			AtomicInteger requestCount = new AtomicInteger(0);
			final Function<Request, CompletableFuture<ScraperResult<T>>> pageRequestFunction = req->{
				requestCount.incrementAndGet();
				return origPageRequestFunction.apply(req);
			};
			final ScheduledFuture<?> deadlineTicker;
			boolean lastPageReached;
			int firstPageNo = initialPages2.get(0);
			int lastPageNo;
			int lastCumulativeCompletedPageNo = firstPageNo -1;
			int retryCountTotal = 0;
			
			PaginationRequestWorker(){
				synchronized(lock) {
					finalResult = new CompletableFuture<PaginationResult<T>>() {
						private void triggerDone(boolean success) {
							if (done.compareAndSet(false, true)) {
								//We are the one to trigger done.
								List<CompletableFuture<?>> futuresCopy;
								synchronized(lock) {
									deadlineTicker.cancel(false);
									futuresCopy = new ArrayList<>(futures.values());
								}
								for (CompletableFuture<?> f : futuresCopy) {
									if (f != null) {
										if (success) {
											f.complete(null);
										} else {
											f.cancel(true);	
										}
									}
								}
							}
						}
						@Override
						public boolean complete(PaginationResult<T> value) {
							triggerDone(true);
							return super.complete(value);
						}
						@Override
						public boolean cancel(boolean mayInterruptIfRunning) {
							triggerDone(false);
							return super.cancel(mayInterruptIfRunning);
						}
						@Override
						public boolean completeExceptionally(Throwable ex) {
							triggerDone(false);
							return super.completeExceptionally(ex);
						}
					};
					
					deadlineTicker = scheduler.scheduleAtFixedRate(()->{
						checkDone();
					}, 10, 10_000, TimeUnit.MILLISECONDS); //Check every 10 seconds.
				}
				
			}
			private boolean checkDone() {
				if (done.get()) {
					deadlineTicker.cancel(false);
					return true;
				} else if (System.currentTimeMillis() >= effectiveDeadlineMillis) {
					Throwable ex;
					try {
						throw new TimeoutException("deadline reached");
					} catch (TimeoutException e) {
						ex = e;
					}
					finalResult.completeExceptionally(ex); //This result will cancel the timer as well.
					return true;
				} else {
					return false;
				}
			}
			
			private synchronized void requestPage(int pageNo, int retryNo) {
				if (checkDone()) return;
				if (retryNo >= maxRetries) throw new IllegalArgumentException();
				final Request req;
				boolean alreadyExists = futures.containsKey(pageNo);
				if (retryNo == 0) {
					if (alreadyExists) throw new IllegalStateException();
					req = pageCreateFunction.apply(pageNo);
					if (req == null) throw new NullPointerException();
					cachedRequests.put(pageNo, req);
					retryCounts.put(pageNo, retryNo);
				} else {
					if (!alreadyExists) throw new IllegalStateException();
					req = cachedRequests.get(pageNo);
					retryCounts.put(pageNo, retryNo);
					retryCountTotal++;
				}
				if (req == null) throw new IllegalStateException();
				class InnerFutures{
					CompletableFuture<Void> delayFuture = null;
					volatile CompletableFuture<ScraperResult<T>> pageRequestFuture = null;
				}
				final InnerFutures innerFutures = new InnerFutures();
				final CompletableFuture<ScraperResult<T>> future;
				if (retryNo == 0) {
					future = innerFutures.pageRequestFuture = pageRequestFunction.apply(req);
					futures.put(pageNo, future);
				} else {
					long delayMillis = retryMinDelayMillis + (long)(Math.random() * (retryMaxDelayMillis - retryMinDelayMillis));
					innerFutures.delayFuture = ScraperUtil.scheduleDelay(scheduler, delayMillis);
					future = innerFutures.delayFuture.thenCompose((void_)->{
						innerFutures.pageRequestFuture = pageRequestFunction.apply(req);
						return innerFutures.pageRequestFuture;
					});
					futures.put(pageNo, future);
				}
				future.whenComplete((result, throwable) -> {
					if (throwable == null && result == null) {
						//We were pre-empted. We were one of the last pages, and are no longer required.
						//Stop our inner futures by making them "complete".
						if (innerFutures.delayFuture != null) {
							innerFutures.delayFuture.complete(null);
						}
						if (innerFutures.pageRequestFuture != null) {
							innerFutures.pageRequestFuture.complete(null);
						}
						return; //Don't do any of the other logic, and don't synchronize with the lock (or you may get deadlock).
					}
					if (done.get()) return; //Test without lock first.
					
					boolean resultSet = result != null;
					//Do our tests before acquiring synchronized block, so we don't hold up the lock.
					boolean goodResultTestValue = resultSet && (goodResultTest == null || goodResultTest.test(result));
					boolean lastPageTestValue = resultSet && (requestNextPages ? (goodResultTestValue ? lastPageTest.test(result) : false) : initialPages2.get(initialPages2.size()-1) == pageNo);
					
					//Check the lock a second time.
					if (done.get()) return;
					PaginationResult<T> pres  = null; //Completed result.
					Throwable t = null; //Failed result.
					synchronized(lock) {
						if (checkDone()) return; //Check the lock a third time.
						
						if (resultSet) {
							//Is result good?
							if (goodResultTestValue) {
								//Yes, this is good.
								//Check if last page reached.
								if (!lastPageReached) {
									lastPageReached = lastPageTestValue;
									if (lastPageReached) {
										//System.out.println("last page reached");
										lastPageNo = pageNo;
										previouslyKnownLastPage.set(lastPageNo);
									}
								}
								boolean allDone = false;
								if (lastPageReached) {
									
									//Check if all the pages up to lastPageNo are done.
									if (requestNextPages) {
										for (int i = lastCumulativeCompletedPageNo + 1; i <= lastPageNo; i++) {
											CompletableFuture<ScraperResult<T>> f = futures.get(i);
											if (f != null && f.isDone() && !f.isCompletedExceptionally()) {
												lastCumulativeCompletedPageNo = i;
											} else {
												break;
											}
										}
									} else {
										for (int ix = initialPages2.indexOf(lastCumulativeCompletedPageNo) + 1; ix < initialPages2.size(); ix++) {
											int i = initialPages2.get(ix);
											CompletableFuture<ScraperResult<T>> f = futures.get(i);
											if (f != null && f.isDone() && !f.isCompletedExceptionally()) {
												lastCumulativeCompletedPageNo = i;
											} else {
												break;
											}
										}
									}
									
									allDone = lastCumulativeCompletedPageNo == lastPageNo;
									//System.out.println("all done " + allDone + " " + lastCumulativeCompletedPageNo + " " + lastPageNo);
									if (allDone) {
										//We reached the end!
										//Collect all our results and set done.
										List<ScraperResult<T>> scraperResults = new ArrayList<>();
										List<Integer> pageNumbers = new ArrayList<>();
										List<Integer> retryCounts2 = new ArrayList<>();
										for (Entry<Integer, CompletableFuture<ScraperResult<T>>> entry : futures.entrySet()) {
											int i = entry.getKey();
											CompletableFuture<ScraperResult<T>> f = entry.getValue();
											if (i <= lastPageNo) {
												ScraperResult<T> r = f.join();
												if (pageNumbers.isEmpty() || emptyPageTest == null || !emptyPageTest.test(r)) {
													scraperResults.add(r);
													pageNumbers.add(i);
													retryCounts2.add(retryCounts.get(i));
												}
											} else {
												//Complete with null if not already done.
												//This is to signal to it to stop fetching.
												//f.complete(null);
											}
											
										}
										pres = new PaginationResult<T>(pageNumbers, scraperResults, retryCounts2, requestCount.get(), retryCountTotal, createTimeMillis, System.currentTimeMillis());
										//finalResult.complete(pres);
									}
								}
								if (!allDone && requestNextPages && !lastPageReached) {
									//Go ask for more pages if necessary.
									for (int nextPageNo = pageNo + 1; nextPageNo < pageNo + batchSize; nextPageNo++) {
										if (!futures.containsKey(nextPageNo)) {
											requestPage(nextPageNo, 0);
											if (nextPageNo == previouslyKnownLastPageValue) {
												//Hmm, pause here, let's see what the last page gives us.
												break;
											}
										} else if (nextPageNo == previouslyKnownLastPageValue) {
											//Hmm, should we go beyond the next page?
											break; //No.
										}
									}
								}
							} else {
								//No. Need to retry if possible.
								int nextRetryNo = retryNo + 1;
								if (nextRetryNo >= maxRetries) {
									try {
										throw new IOException("bad result, max retries reached for page " + pageNo);
									} catch (Exception e) {
										t = e;
										//finalResult.completeExceptionally(e);
									}
								} else {
									requestPage(pageNo, nextRetryNo); //Retry again.
								}
							}
						} else if (throwable != null) {
							//Need to retry if possible.
							int nextRetryNo = retryNo + 1;
							
							if (nextRetryNo < maxRetries && (retryOnErrorTest == null || retryOnErrorTest.test(req, throwable))) {
								//Should retry.
								requestPage(pageNo, nextRetryNo); //Retry again.
							} else {
								//Shouldn't retry.. fail.
								t = throwable; //finalResult.completeExceptionally(throwable);
							}
						}
					}
					
					//Run complete & cancel outside of synchronized block.
					if (pres != null) {
						finalResult.complete(pres);
					} else if (t != null) {
						finalResult.completeExceptionally(t);
					}
				});
			}
		}
		PaginationRequestWorker worker = new PaginationRequestWorker();
		synchronized(worker.lock) {
			//Request initial pages.
			for (int pageNo : initialPages) {
				worker.requestPage(pageNo, 0);
				if (pageNo == previouslyKnownLastPageValue) {
					break; //Stop here for now.
				}
			}
		}
		return worker.finalResult;
	}
	
	public CompletableFuture<PaginationResult<T>> requestPages(final long deadlineMillis){
		List<Integer> initialPages = new ArrayList<>(batchSize);
		for (int i = 0; i < batchSize; i++) {
			initialPages.add(i);
		}
		return requestPages(deadlineMillis, initialPages, true);
	}
	
	public CompletableFuture<PaginationResult<T>> requestSelectedPages(final long deadlineMillis, final List<Integer> pageNumbers){
		return requestPages(deadlineMillis, new ArrayList<Integer>(pageNumbers), false);
	}
	

}
