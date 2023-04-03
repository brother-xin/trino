package io.trino.tdengine;

import java.util.HashMap;

public class TdEngineRow {

	private HashMap<String, Object> columnMap;

	public TdEngineRow() {
	}

	public TdEngineRow(long timestamp, String value, HashMap<String, Object> columnMap) {
		super();
		this.columnMap = columnMap;
		this.columnMap.put("timestamp", timestamp + "");
		this.columnMap.put("value", value);
	}

	public HashMap<String, Object> getColumnMap() {
		return columnMap;
	}

	public void setColumnMap(HashMap<String, Object> columnMap) {
		this.columnMap = columnMap;
	}

}
