package astar.ihpc.umgc.umgcscraper.util;

import java.util.concurrent.locks.LockSupport;

/**
 * Helper to sleep the current thread and wake up based on a realtime schedule.
 * 
 * The stepper has no internal state and can be safely used by multiple threads.
 * @author othmannb
 *
 */
public final class RealTimeStepper {
	static {
		ScraperUtil.enableHiResSleep();
	}
	private final long timeStepMillis;
	private final long startTimeMillis;
	private final long maxOvershootMillis;
	private final long maxRandomDelayMillis;
	
	/**
	 * Constructs a new TimeStepper.
	 * @param startTimeMillis the time of the first tick, as milliseconds since Unix Epoch (1 Jan 1970 UTC).
	 * @param timeStepMillis the time step in milliseconds
	 * @param maxOvershootMillis the maximum allowed delay of a time step, before the step is skipped
	 * @param maxRandomDelayMillis the maximum random delay to add to a time step, useful to avoid multiple identical TimeSteppers (in different programs) stepping at exactly same time
	 */
	public RealTimeStepper(long startTimeMillis, long timeStepMillis, long maxOvershootMillis, long maxRandomDelayMillis) {
		if (timeStepMillis < 4) throw new IllegalArgumentException("timeStepMillis must be at least 4 ms");
		if (maxOvershootMillis <= 0) throw new IllegalArgumentException("maxOvershootMillis must be greater than zero");
		if (maxOvershootMillis > timeStepMillis / 2) throw new IllegalArgumentException("maxOvershootMillis must not be greater than half of timeStepMillis");
		if (maxRandomDelayMillis < 0) throw new IllegalArgumentException("maxRandomDelayMillis must not be less than zero");
		if (maxRandomDelayMillis >= maxOvershootMillis) throw new IllegalArgumentException("maxRandomDelayMillis must be strictly less than maxOvershootMillis");
		
		this.timeStepMillis = timeStepMillis;
		this.startTimeMillis = startTimeMillis;
		this.maxOvershootMillis = maxOvershootMillis;
		this.maxRandomDelayMillis = maxRandomDelayMillis;
	}
	/**
	 * The absolute start time in milliseconds since epoch. The start time can be (and should be) in the past.
	 * If the start time is in the future, it still works, but it may not be the very first step to execute
	 * For example, if start time is 30 minutes into the future, but timestep is 1 minute, then the very first tick will happen within the next minute
	 * and not 30 minutes later.
	 * @return
	 */
	public long getStartTimeMillis() {
		return startTimeMillis;
	}
	/**
	 * The time span between consecutive steps, in milliseconds.
	 * The actual time span between two actual ticks may vary a little bit due to delays (system + specified random delay).
	 * @return
	 */
	public long getTimeStepMillis() {
		return timeStepMillis;
	}
	/**
	 * The maximum allowable delay for a step before we are too late to step to it.
	 * For example, if the time step is 1 minute with a max overshoot of 10 seconds; if we are late to the time step by 11 seconds, we are
	 * considered to be beyond the allowable delay.
	 * Steps that are beyond the overshoot are skipped rather than executed, as it is assumed that maintaining the rhythm is more important
	 * than not missing any steps.
	 * @return
	 */
	public long getMaxOvershootMillis() {
		return maxOvershootMillis;
	}
	/**
	 * The maximum random delay that this stepper can introduce artificially.
	 * If zero, there is no artificial delay, and the stepper attempts to step precisely (but may still be delayed due to OS scheduling / CPU, etc).
	 * If non-zero, the stepper adds a random delay from 0 to max random delay at each time step.
	 * 
	 * The reason why a random delay is desirable is to avoid bursting steps when you have multiple programs running which can coincide their steps at the exact millisecond.
	 * For example, if you have a scraper script that runs every minute, and another that runs every 30 seconds, then every 60 seconds both independent programs may
	 * step at precisely the same millisecond, and causing extra CPU load to both the local machine and possibly to the remote server.
	 * 
	 * This is especially the case if the OS is very reliable at time-keeping (like Linux).
	 * 
	 * @return
	 */
	public long getMaxRandomDelayMillis() {
		return maxRandomDelayMillis;
	}
	
	public long calcCurrentStepMillis() {
		long now = System.currentTimeMillis();
		long prevStepMs = (now - startTimeMillis) / timeStepMillis * timeStepMillis + startTimeMillis;
		return prevStepMs;
	}
	
	public long calcNextStepMillis() {
		long now = System.currentTimeMillis();
		long prevStepMs = (now - startTimeMillis) / timeStepMillis * timeStepMillis + startTimeMillis;
		long nextStepMs = prevStepMs + timeStepMillis;
		return nextStepMs;
	}
	/**
	 * Sleep the current thread until the next step. This sleep is not interruptible, the only way the method returns is if it reaches the next step.
	 */
	public void nextStep() {
		//Assign a random delay if necessary for this step.
		long randomDelayMillis = maxRandomDelayMillis == 0 ? 0 : (long)(Math.random() * maxRandomDelayMillis);
		while (true) {
			long now = System.currentTimeMillis();
			long prevStepMs = (now - startTimeMillis) / timeStepMillis * timeStepMillis + startTimeMillis;
			long nextStepMs = prevStepMs + timeStepMillis;
			long nextEffectiveStepMs = nextStepMs + randomDelayMillis;
			long nextOvershootMs = nextStepMs + maxOvershootMillis;
			
			while (now >= prevStepMs && now < nextEffectiveStepMs){
				long totalSleepMs = nextEffectiveStepMs - now;
				if (totalSleepMs == 1) {
					//1ms or less, parkNanos() for 0.350 ms at a time.
					LockSupport.parkNanos(350_000L); //350 microseconds = 0.35 ms
				} else {
					//Sleep for the time required to the next step, but up to 30 seconds at a time.
					//We don't sleep full duration because who knows if the system clock is changed by user or NTP.
					//At least by sleeping 30 seconds at a time, we will re-consult system clock often enough.
					LockSupport.parkNanos(Math.min(30_000_000_000L, totalSleepMs * 1_000_000L));
				}
				Thread.interrupted(); //Clear interrupted flag if any (ignore interruptions).
				now = System.currentTimeMillis();
			}
			//Are we safely within the time step?
			if (now >= nextEffectiveStepMs && now < nextOvershootMs) {
				//Yes, we can tick.
				return;
			} else {
				//Either we overshoot, or the system clock was changed (somehow).
				//Do next step then.
			}
		}
	}
	
	/**
	 * Sleep the current thread until the next step, or the thread is interrupted.
	 * If the method returns without throwing an {@link InterruptedException}, the sleep was successful.
	 * @throws InterruptedException if the thread is interrupted.
	 */
	public void nextStepInterruptibly() throws InterruptedException {
		//Assign a random delay if necessary for this step.
		long randomDelayMillis = maxRandomDelayMillis == 0 ? 0 : (long)(Math.random() * maxRandomDelayMillis);
		while (true) {
			long now = System.currentTimeMillis();
			long prevStepMs = (now - startTimeMillis) / timeStepMillis * timeStepMillis + startTimeMillis;
			long nextStepMs = prevStepMs + timeStepMillis;
			long nextEffectiveStepMs = nextStepMs + randomDelayMillis;
			long nextOvershootMs = nextStepMs + maxOvershootMillis;
			
			while (now >= prevStepMs && now < nextEffectiveStepMs){
				long totalSleepMs = nextEffectiveStepMs - now;
				if (totalSleepMs == 1) {
					//1ms or less, parkNanos() for 0.350 ms at a time.
					LockSupport.parkNanos(350_000L); //350 microseconds = 0.35 ms
				} else {
					//Sleep for the time required to the next step, but up to 30 seconds at a time.
					//We don't sleep full duration because who knows if the system clock is changed by user or NTP.
					//At least by sleeping 30 seconds at a time, we will re-consult system clock often enough.
					LockSupport.parkNanos(Math.min(30_000_000_000L, totalSleepMs * 1_000_000));
				}
				if (Thread.interrupted()) {
					throw new InterruptedException();
				}
				now = System.currentTimeMillis();
			}
			//Are we safely within the time step?
			if (now >= nextEffectiveStepMs && now < nextOvershootMs) {
				//Yes, we can tick.
				return;
			} else {
				//Either we overshoot, or the system clock was changed (somehow).
				//Do next step then.
			}
		}
	}
}
