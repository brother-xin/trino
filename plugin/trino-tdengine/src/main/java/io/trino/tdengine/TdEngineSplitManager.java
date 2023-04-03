package io.trino.tdengine;

import io.trino.spi.connector.*;

import java.util.ArrayList;
import java.util.List;

public class TdEngineSplitManager implements ConnectorSplitManager {

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint) {
        TdEngineTableHandle tableHandle = (TdEngineTableHandle) connectorTableHandle;
        //TdEngineTable table = client.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());

        // this can happen if table is removed during a query
        //if (table == null) {
            //throw new TableNotFoundException(tableHandle.toSchemaTableName());
        //}

        List<ConnectorSplit> splits = new ArrayList<>();
        splits.add(new TdEngineSplit(tableHandle.getSchemaName(), tableHandle.getTableName()));
        return new FixedSplitSource(splits);
    }
}
