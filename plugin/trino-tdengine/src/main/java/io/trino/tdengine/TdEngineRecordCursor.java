package io.trino.tdengine;


import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

public class TdEngineRecordCursor implements RecordCursor {
    public static Connection connection;
    private List<TdEngineColumnHandle> columnHandles;
    private Iterator<TdEngineRow> iterator;
    private TdEngineRow row;


    public List<TdEngineColumnHandle> getColumnHandles() {
        return columnHandles;
    }

    public void setColumnHandles(List<TdEngineColumnHandle> columnHandles) {
        this.columnHandles = columnHandles;
    }

    public Iterator<TdEngineRow> getIterator() {
        return iterator;
    }

    public void setIterator(Iterator<TdEngineRow> iterator) {
        this.iterator = iterator;
    }

    public TdEngineRow getRow() {
        return row;
    }

    public void setRow(TdEngineRow row) {
        this.row = row;
    }

    public TdEngineRecordCursor(List<TdEngineColumnHandle> columnHandles, TdEngineSplit split) throws Exception {
        this.columnHandles = columnHandles;
        this.iterator = new TdEngineClient(connection).select(split.getTableName(), split.getSchemaName());
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition() {
        if (!iterator.hasNext()) {
            return false;
        }
        this.row = iterator.next();
        // log.info("tdengineRecordCursor row :" + this.row.getColumnMap());
        return true;
    }

    @Override
    public boolean getBoolean(int field) {
        String columnName = columnHandles.get(field).getColumnName();
        return Boolean.valueOf(row.getColumnMap().get(columnName) + "");
    }

    @Override
    public long getLong(int field) {
        String columnName = columnHandles.get(field).getColumnName();
        return Long.valueOf(row.getColumnMap().get(columnName) + "");
    }


    @Override
    public double getDouble(int field) {
        String columnName = columnHandles.get(field).getColumnName();
        return Double.valueOf(row.getColumnMap().get(columnName) + "");
    }

    @Override
    public Slice getSlice(int field) {
        String columnName = columnHandles.get(field).getColumnName();
        Object o = row.getColumnMap().get(columnName);
        if (o == null) {
            return Slices.utf8Slice("");
        }
        if (o instanceof Timestamp) {
            return Slices.utf8Slice(o + "");
        }
        if (o instanceof byte[]) {
            byte[] bytes = (byte[]) o;
            return Slices.wrappedBuffer(bytes, 0, bytes.length);
        }
        return Slices.utf8Slice((String) o);
    }

    @Override
    public Object getObject(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field) {
        String columnName = columnHandles.get(field).getColumnName();
        Object o = row.getColumnMap().get(columnName);
        return o == null;
    }

    @Override
    public void close() {

    }
}
