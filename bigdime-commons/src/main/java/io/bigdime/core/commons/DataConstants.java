/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

/**
 */
public final class DataConstants {
	private static final DataConstants instance = new DataConstants();
	private DataConstants (){}
	
	public static DataConstants getInstance() {
		return instance;
	}


	public static final String CTRL_A =  "\u0001";
	public static final String EOL = "\n";
	public static final String SLASH = "/";
	public static final String COLUMN_SEPARATED_BY =  "columnSeparatedBy";
	public static final String ROW_SEPARATED_BY =  "rowSeparatedBy";
	public static final String SCHEMA_FILE_NAME = "schemaFileName";
	public static final String ENTITY_NAME = "entity-name";
	public static final String COLON = ":";
	public static final String COMMA = ",";
	public static final String AND = " AND ";
	
	public static final String EQUAL = "=";
	public static final String BACK_SLASH = "\"";


}
