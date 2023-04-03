package io.trino.tdengine;

public class TdEngineTable {
    private String schemaName;
    private String tableName;

    public TdEngineTable(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }
}
