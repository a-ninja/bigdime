/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;

import com.google.common.base.Preconditions;

public final  class ColumnMetaDataUtil {
	private ColumnMetaDataUtil(){}
	public static List<Column> addColumns(List<HCatFieldSchema> hcatColumns) {
		Preconditions.checkNotNull(hcatColumns);
		List<Column> columns = new ArrayList<Column>();
		for (HCatFieldSchema hcatColumn : hcatColumns) {
			Column column = new Column (hcatColumn.getName(), 
					hcatColumn.getTypeString(), hcatColumn.getComment());
			columns.add(column);
		}
		return columns;
	}

	public static ArrayList<HCatFieldSchema> prepareHiveColumn(List<Column> sourceCols) throws HCatException {
		ArrayList<HCatFieldSchema> targetCols = new ArrayList<HCatFieldSchema>();
		Preconditions.checkNotNull(sourceCols);
		// Note: Add other column types as needed.
		for (Column column : sourceCols) {
			targetCols.add(new HCatFieldSchema(column.getName(),
					SqlTypes2HiveTypes.sqlType2HiveType(column.getType()),
					column.getComment()));

		}
		return targetCols;
	}
}
