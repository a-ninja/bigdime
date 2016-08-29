package io.bigdime.core.commons;

import java.util.concurrent.TimeUnit;

public class DateTimeHelper {

	/**
     * @formatter:off
     * Use case: 1
	 * If the duration is 1 hour, and right now it's 4.10pm, and latency is 1 hour:
	 * timeNow = 4.10 pm
	 * 
	 * processTime = timeNow - duration - latency
	 * processTime = 2.10pm.
	 * get hour = 2 pm
	 * Set the processing time as 2pm.
	 * 
	 * 
	 * 
	 * @formatter:on
	 * 
	 * Say we have to compute the hour for which we have to run a feed.
	 * At or after 4 PM, the feed needs to be run for the data updated before 4 and after 3(or whatever time in past).
	 * @param timeUnit
	 * @param qty
	 */
	public static long compute(TimeUnit timeUnit, int duration, int latency) {

		long current = System.currentTimeMillis();
		
		long goBackBy = timeUnit.toMillis(duration);
		long pastTime = current - goBackBy;
		
		return pastTime;
	}
}
