package io.trino.tdengine;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TdEngineColumnHandle implements ColumnHandle {
    private final String columnName;
    private final Type columnType;
    private final int ordinalPosition;

    @JsonCreator
    public TdEngineColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("ordinalPosition") int ordinalPosition) {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.ordinalPosition = ordinalPosition;
    }

    @JsonProperty
    public String getColumnName() {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType() {
        return columnType;
    }

    @JsonProperty
    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public ColumnMetadata getColumnMetadata() {
        return new ColumnMetadata(columnName, columnType);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .add("ordinalPosition", ordinalPosition)
                .toString();
    }
}
