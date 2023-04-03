package io.trino.tdengine;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TdEngineRecordSet implements RecordSet {
    private List<TdEngineColumnHandle> columnHandles;
    private List<Type> columnTypes;
    private TdEngineSplit split;

    public List<TdEngineColumnHandle> getColumnHandles() {
        return columnHandles;
    }

    public void setColumnHandles(List<TdEngineColumnHandle> columnHandles) {
        this.columnHandles = columnHandles;
    }

    public void setColumnTypes(List<Type> columnTypes) {
        this.columnTypes = columnTypes;
    }

    public TdEngineSplit getSplit() {
        return split;
    }

    public void setSplit(TdEngineSplit split) {
        this.split = split;
    }

    public TdEngineRecordSet(TdEngineSplit split, List<TdEngineColumnHandle> columnHandles) {
        this.split = requireNonNull(split, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (TdEngineColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
    }

    //该方法获取一行数据中所有的列的类型
    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    //该方法需要返回一个RecordCursor的实体对象。该对象中定义读取数据的逻辑
    @Override
    public RecordCursor cursor() {
        try {
            return new TdEngineRecordCursor(columnHandles, split);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
