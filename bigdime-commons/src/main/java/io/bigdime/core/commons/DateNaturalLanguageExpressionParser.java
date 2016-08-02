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

	/**
	 * expression: 1 min | mins | minute | minutes | hour | hours | day | days
	 * Convert to milliseconds and return.
	 * 
	 * @param expression
	 */
	public static long toMillis(final String expression) {
		String pattern = "(\\d+)\\s*(\\w+)";
		Pattern p = Pattern.compile(pattern);

		Matcher m = p.matcher(expression);
		long millis = 0;
		while (m.find()) {
			int groups = m.groupCount();
			int num = Integer.valueOf(m.group(1));
			String unit = m.group(2);

			TimeUnit timeUnit = TIME_UNIT_NAME.getByValue(unit);
			millis = timeUnit.toMillis(num);
			System.out.println("timeUnit=" + timeUnit + ", millis = " + millis);

			for (int i = 0; i < groups; i++) {
				System.out.println(m.group(i + 1));
			}
		}
		return millis;
	}
}
