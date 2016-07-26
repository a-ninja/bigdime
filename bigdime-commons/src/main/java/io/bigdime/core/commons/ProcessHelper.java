package io.bigdime.core.commons;

import java.lang.management.ManagementFactory;

public enum ProcessHelper {
	INSTANCE;

	public static ProcessHelper getInstance() {
		return ProcessHelper.INSTANCE;
	}

	public String getProcessId() {

		try {
			final String jvmName = ManagementFactory.getRuntimeMXBean().getName();
			final int index = jvmName.indexOf('@');

			if (index < 1) {
				return "unknown";
			}

			try {
				return Long.toString(Long.parseLong(jvmName.substring(0, index)));
			} catch (NumberFormatException e) {
				System.err.println("unable to obtain the process_id, not a number: " + e.getMessage());
				return "unknown";
			}
		} catch (final Exception ex) {
			System.err.println("unable to obtain the process_id: " + ex.getMessage());
			return "unknown";
		}
	}

}
