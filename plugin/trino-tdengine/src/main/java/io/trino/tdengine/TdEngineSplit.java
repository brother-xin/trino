package io.trino.tdengine;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.ArrayList;
import java.util.List;

public class TdEngineSplit implements ConnectorSplit {
    private String schemaName;
    private String tableName;

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @JsonCreator
    public TdEngineSplit(@JsonProperty("schemaName") String schemaName, @JsonProperty("tableName") String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return new ArrayList<>();
    }

    @Override
    public Object getInfo() {
        return this;
    }
}
