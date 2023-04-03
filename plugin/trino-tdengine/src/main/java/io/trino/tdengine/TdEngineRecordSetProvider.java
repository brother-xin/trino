package io.trino.tdengine;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.*;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.logging.Logger;

public class TdEngineRecordSetProvider implements ConnectorRecordSetProvider {

    private Logger log = Logger.getLogger(this.getClass().getName());

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction
            , ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table
            , List<? extends ColumnHandle> columns) {

        TdEngineSplit tdEngineSplit = (TdEngineSplit) split;

        ImmutableList.Builder<TdEngineColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            TdEngineColumnHandle tdEngineColumnHandle = (TdEngineColumnHandle) handle;
            String columnName = tdEngineColumnHandle.getColumnName();
            Type columnType = tdEngineColumnHandle.getColumnType();
            log.info(" [recordSetProvider]tdengine column handle,columnName " + columnName + ",columnType:" + columnType);
            handles.add(tdEngineColumnHandle);
        }

        return new TdEngineRecordSet(tdEngineSplit, handles.build());
    }
}
