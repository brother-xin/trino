package io.trino.tdengine;

import io.trino.spi.connector.*;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;
import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;

public class TdEngineConnectorMetadata implements ConnectorMetadata {
    Logger log = Logger.getLogger(this.getClass().getName());
    private final TdEngineClient client;

    @Inject
    public TdEngineConnectorMetadata(TdEngineClient tdEngineClient) {
        this.client = requireNonNull(tdEngineClient, "exampleClient is null");
    }

    //该方法对应Presto show schemas命令，用来展示Connector下有哪些数据库
    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        log.info("tdengine connector metadata list schemas begin ---");
        List<String> schemaNames = client.listSchemaNames();
        log.info("tdengine connector metadata list schemas end,result:" + schemaNames.toString());
        return schemaNames;
    }

    @Nullable
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
//        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
//            return null;
//        }
//        TdEngineTable table = client.getTable(tableName.getSchemaName(), tableName.getTableName());
//        if (table == null) {
//            return null;
//        }
        return new TdEngineTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        log.info("tdengine connector metadata getTableMetadata begin ---");
        TdEngineTableHandle tableHandle = (TdEngineTableHandle) table;
        List<ColumnMetadata> list;
        try {
            list = client.getColumnMetadata(tableHandle.getSchemaName(), tableHandle.getTableName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        SchemaTableName tableName = new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName());
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, list);
        log.info("tdengine connector metadata getTableMetadata over ---");
        return tableMetadata;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        log.info("tdengine connector metadata listTables begin ---");
        List<SchemaTableName> listTable = new ArrayList<>();
        if (schemaName.isPresent()) {
            for (String table : client.getStable(schemaName.get())) {
                listTable.add(new SchemaTableName(schemaName.get(), table));
            }
        }
        log.info("tdengine connector metadata listTables over ---");
        return listTable;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        log.info("tdengine connector metadata getColumnHandles begin ---");
        TdEngineTableHandle tableHandle = (TdEngineTableHandle) table;
        Map<String, ColumnHandle> map = new HashMap<>();
        List<ColumnMetadata> list = client.getColumnMetadata(tableHandle.getSchemaName(), tableHandle.getTableName());
        for (int i = 0; i < list.size(); i++) {
            ColumnMetadata meta = list.get(i);
            map.put(meta.getName(), new TdEngineColumnHandle(meta.getName(), meta.getType(), i));
        }
        log.info("tdengine connector metadata getColumnHandles over ---");
        return map;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        log.info("tdengine connector metadata listTableColumns begin ---");
        Map<SchemaTableName, List<ColumnMetadata>> columns = new HashMap<>();
        Optional<String> optional = Optional.of(prefix.getSchema().get());
        List<SchemaTableName> list = listTables(session, optional);
        for (SchemaTableName table : list) {
            if (table.getTableName().startsWith(prefix.getTable().get())) {
                columns.put(table, client.getColumnMetadata(table.getSchemaName(), table.getTableName()));
            }
        }
        log.info("tdengine connector metadata listTableColumns over ---");
        return columns;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return ((TdEngineColumnHandle) columnHandle).getColumnMetadata();
    }

}
