/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation;

/**
 * @author jbrinnand, Neeraj Jain
 */
public final class DataValidationConstants {
	private static final DataValidationConstants instance = new DataValidationConstants();

	private DataValidationConstants() {
	}

	public static DataValidationConstants getInstance() {
		return instance;
	}

	public static final int CHECKSUM_BYTE_PER_CRC = 512;
	public static final int CHECKSUM_BLOCK_SIZE = 128 * 1024 * 1024;
	public static final int BUFFER_SIZE_4 = 4;
	public static final int BUFFER_SIZE_512 = 512;
	public static final int BUFFER_SIZE_1024 = 1024;

}