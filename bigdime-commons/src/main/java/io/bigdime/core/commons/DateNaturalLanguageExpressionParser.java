package io.bigdime.core.commons;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateNaturalLanguageExpressionParser {

	enum TIME_UNIT_NAME {
		SEC(TimeUnit.SECONDS),

		/**
		 * 
		 */
		SECS(TimeUnit.SECONDS),

		/**
		 * 
		 */
		SECOND(TimeUnit.SECONDS),

		/**
		 * 
		 */
		SECONDS(TimeUnit.SECONDS),

		MIN(TimeUnit.MINUTES),

		/**
		 * 
		 */
		MINS(TimeUnit.MINUTES),

		/**
		 * 
		 */
		MINUTE(TimeUnit.MINUTES),
		/**
		 * 
		 */
		MINUTES(TimeUnit.MINUTES),

		/**
		 * 
		 */
		HOUR(TimeUnit.HOURS),

		/**
		 * 
		 */
		HOURS(TimeUnit.HOURS),

		/**
		 * 
		 */
		DAY(TimeUnit.DAYS),

		/**
		 * 
		 */
		DAYS(TimeUnit.DAYS);

		private TimeUnit timeUnit;

		public static TimeUnit getByValue(String val) {
			if (val == null)
				return null;
			String temp = val.toUpperCase();
			for (TIME_UNIT_NAME obj : TIME_UNIT_NAME.values()) {
				if (obj.name().equals(temp)) {
					return obj.timeUnit;
				}
			}
			return null;
		}

		private TIME_UNIT_NAME(TimeUnit timeUnit) {
			this.timeUnit = timeUnit;
		}

	}

	public static Object[] getDurationAndTimeUnitFromExpression(final String expression) {
		Object[] returnVal = new Object[2];
		String pattern = "(\\d+)\\s*(\\w+)";
		Pattern p = Pattern.compile(pattern);
		Matcher m = p.matcher(expression);
		while (m.find()) {
			returnVal[0] = Long.valueOf(m.group(1));
			returnVal[1] = TIME_UNIT_NAME.getByValue(m.group(2));
		}
		return returnVal;
	}

	/**
	 * expression: 1 min | mins | minute | minutes | hour | hours | day | days
	 * Convert to milliseconds and return.
	 * 
	 * @param expression
	 */
	public static long toMillis(final String expression) {
		Object[] durationAndTimeUnit = getDurationAndTimeUnitFromExpression(expression);
		long duration = Long.valueOf((Long) durationAndTimeUnit[0]);
		TimeUnit timeUnit = (TimeUnit) durationAndTimeUnit[1];
		return timeUnit.toMillis(duration);
	}

	/**
	 * 
	 * @param currentMillis
	 * @param expression
	 */
	public static long getPastDate(long currentMillis, final String goPastExpression) {
		long goBackMillis = toMillis(goPastExpression);
		long pastMillis = currentMillis - goBackMillis;
		return pastMillis;
	}
}
