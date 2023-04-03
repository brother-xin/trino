package io.trino.tdengine;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.utils.StringUtils;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.*;

import javax.inject.Inject;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class TdEngineClient {

    private final Connection conn;

    @Inject
    public TdEngineClient(Connection connection) {
        this.conn = connection;
    }

    public List<String> listSchemaNames() {
        List<String> databases = new ArrayList<>();
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet catalogs = metaData.getCatalogs();
            while (catalogs.next()) {
                String string = catalogs.getString(1);
                databases.add(string);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return databases.stream().distinct().collect(Collectors.toList());
    }

    public List<ColumnMetadata> getColumnMetadata(String schemaName, String tableName) {
        List<ColumnMetadata> metadataList = new ArrayList<>();
        try {
            ResultSet columns = conn.getMetaData().getColumns(schemaName, "", tableName, "");
            while (columns.next()) {
                String filed = columns.getString(4);
                String type = columns.getString(5);
                String taosTypeName = "varchar";
                try {
                    taosTypeName = TSDBConstants.jdbcType2TaosTypeName(Integer.parseInt(type));
                } catch (Exception e) {
                }
                ColumnMetadata column = new ColumnMetadata(filed, taosTypeName2TrinoType(taosTypeName));
                metadataList.add(column);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return metadataList;
    }

    private static Type taosTypeName2TrinoType(String taosTypeName) {
        switch (taosTypeName) {
            case "INT":
                return IntegerType.INTEGER;
            case "BIGINT":
                return BigintType.BIGINT;
            case "FLOAT":
            case "DOUBLE":
                return DoubleType.DOUBLE;
            case "SMALLINT":
                return SmallintType.SMALLINT;
            case "TINYINT":
                return TinyintType.TINYINT;
            case "BOOL":
                return BooleanType.BOOLEAN;
            case "NCHAR":
            case "BINARY":
            case "TIMESTAMP":
            default:
                return VarcharType.createUnboundedVarcharType();
        }
    }


    public List<String> getStable(String dbName) {
        List<String> tables = new ArrayList<>();
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet schemas = metaData.getSuperTables(dbName, "", "%%");
            while (schemas.next()) {
                tables.add(schemas.getString(4));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return tables.stream().filter(t -> !StringUtils.isEmpty(t)).distinct().collect(Collectors.toList());
    }

    public Iterator<TdEngineRow> select(String tableName, String schemaName) throws Exception {
        List<TdEngineRow> list = new ArrayList<>();
        List<ColumnMetadata> columns = getColumnMetadata(schemaName, tableName);
        ResultSet resultSet = getData(schemaName, tableName);
        while (resultSet.next()) {
            TdEngineRow row = new TdEngineRow();
            HashMap<String, Object> hashMap = new HashMap<>();
            for (ColumnMetadata column : columns) {
                hashMap.put(column.getName(), resultSet.getObject(column.getName()));
                row.setColumnMap(hashMap);
            }
            list.add(row);
        }
        return list.iterator();
    }

    public ResultSet getData(Connection conn, String dbName, String tableName) throws SQLException {
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from " + dbName + "." + tableName);
        return resultSet;
    }

    public ResultSet getData(String dbName, String tableName) throws Exception {
        return getData(conn, dbName, tableName);
    }
}
