/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.taosdb;

import com.google.common.collect.ImmutableSet;
import com.taosdata.jdbc.ColumnMetaData;
import com.taosdata.jdbc.DatabaseMetaDataResultSet;
import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBResultSetRowData;
import io.airlift.log.Logger;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.jdbc.*;
import io.trino.plugin.jdbc.aggregation.*;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.type.*;

import javax.inject.Inject;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.*;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.util.Locale.ENGLISH;

public class TaosdbClient extends BaseJdbcClient {
    private static final Logger log = Logger.get(TaosdbClient.class);
    private static final int MAX_SUPPORTED_DATE_TIME_PRECISION = 6;
    private static final int ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE = 19;
    private static final int ZERO_PRECISION_TIME_COLUMN_SIZE = 8;
    private static final String NO_COMMENT = "";

    private final Type jsonType;
    private final boolean statisticsEnabled;
    private final ConnectorExpressionRewriter<String> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, String> aggregateFunctionRewriter;
    private final IdentifierMapping identifierMapping;

    @Inject
    public TaosdbClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier) {
        super("`", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
        this.statisticsEnabled = statisticsConfig.isEnabled();
        this.identifierMapping = identifierMapping;
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .build();

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, String>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .build());
    }

    @Override
    public Collection<String> listSchemas(Connection connection) {
        List<String> databases = new ArrayList<>();
        try {
            DatabaseMetaData metaData = connection.getMetaData();
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

    @Override
    protected ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata) throws SQLException {
        JdbcNamedRelationHandle handle = (JdbcNamedRelationHandle) tableHandle.getRelationHandle();
        ResultSet columns = metadata.getColumns(handle.getSchemaTableName().getSchemaName(), "", handle.getSchemaTableName().getTableName(), "");
        return columns;
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        String catalog = schemaName.get();
        DatabaseMetaDataResultSet tables = (DatabaseMetaDataResultSet) metadata.getSuperTables(catalog, "", "%%");
        List<TSDBResultSetRowData> rowDataList = new ArrayList<>();
        DatabaseMetaDataResultSet set = new DatabaseMetaDataResultSet();
        List<String> superTables = new ArrayList<>();
        while (tables.next()) {
            if (!superTables.contains(tables.getString(4))) {
                TSDBResultSetRowData rowData = new TSDBResultSetRowData(4);
                rowData.setStringValue(1, tables.getString(1));
                rowData.setStringValue(2, tables.getString(2));
                rowData.setStringValue(3, tables.getString(4));
                rowData.setStringValue(4, tables.getString(4));
                rowDataList.add(rowData);
                superTables.add(tables.getString(4));
            }
        }
        set.setRowDataList(rowDataList);
        set.setColumnMetaDataList(buildGetSuperTablesColumnMetaDataList());
        return set;
    }


    @Override
    public Optional<String> getTableComment(ResultSet resultSet) throws SQLException {
        // String remarks = resultSet.getString("REMARKS");
        //TODO  未实现
        return Optional.ofNullable(emptyToNull(""));
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle) {

        log.info("tdengine client v2 column mapping starts .........");
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        switch (jdbcTypeName.toLowerCase(ENGLISH)) {
            case "int":
                return Optional.of(integerColumnMapping());
            case "bigint":
                return Optional.of(bigintColumnMapping());
            case "float":
                return Optional.of(ColumnMapping.longMapping(
                        REAL,
                        (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)),
                        realWriteFunction(),
                        DISABLE_PUSHDOWN));
            case "double":
                return Optional.of(doubleColumnMapping());
            case "smallint":
                return Optional.of(smallintColumnMapping());
            case "tinyint":
                return Optional.of(tinyintColumnMapping());
            case "bool":
                return Optional.of(booleanColumnMapping());
            case "nchar", "binary", "", "timestamp":
                return Optional.of(varcharColumnMapping(VarcharType.VARCHAR, true));
        }
        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.of(varcharColumnMapping(VarcharType.VARCHAR, true));
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type) {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("float", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }

        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", tdEngineDateWriteFunctionUsingLocalDate());
        }

        if (type instanceof TimeType timeType) {
            if (timeType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION) {
                return WriteMapping.longMapping(format("time(%s)", timeType.getPrecision()), timeWriteFunction(timeType.getPrecision()));
            }
            return WriteMapping.longMapping(format("time(%s)", MAX_SUPPORTED_DATE_TIME_PRECISION), timeWriteFunction(MAX_SUPPORTED_DATE_TIME_PRECISION));
        }

        if (type instanceof TimestampType timestampType) {
            if (timestampType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION) {
                verify(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION);
                return WriteMapping.longMapping(format("datetime(%s)", timestampType.getPrecision()), timestampWriteFunction(timestampType));
            }
            return WriteMapping.objectMapping(format("datetime(%s)", MAX_SUPPORTED_DATE_TIME_PRECISION), longTimestampWriteFunction(timestampType, MAX_SUPPORTED_DATE_TIME_PRECISION));
        }

        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("mediumblob", varbinaryWriteFunction());
        }

        if (type instanceof CharType charType) {
            return WriteMapping.sliceMapping("char(" + charType.getLength() + ")", charWriteFunction());
        }

        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "longtext";
            } else if (varcharType.getBoundedLength() <= 255) {
                dataType = "tinytext";
            } else if (varcharType.getBoundedLength() <= 65535) {
                dataType = "text";
            } else if (varcharType.getBoundedLength() <= 16777215) {
                dataType = "mediumtext";
            } else {
                dataType = "longtext";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type.equals(jsonType)) {
            return WriteMapping.sliceMapping("json", varcharWriteFunction());
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    private LongWriteFunction tdEngineDateWriteFunctionUsingLocalDate() {
        return new LongWriteFunction() {
            @Override
            public String getBindExpression() {
                return "CAST(? AS DATE)";
            }

            @Override
            public void set(PreparedStatement statement, int index, long epochDay)
                    throws SQLException {
                statement.setString(index, LocalDate.ofEpochDay(epochDay).format(ISO_DATE));
            }
        };
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition) {
        if (joinCondition.getOperator() == JoinCondition.Operator.IS_DISTINCT_FROM) {
            return false;
        }

        // Remote database can be case insensitive.
        return Stream.of(joinCondition.getLeftColumn(), joinCondition.getRightColumn())
                .map(JdbcColumnHandle::getColumnType)
                .noneMatch(type -> type instanceof CharType || type instanceof VarcharType);
    }

    private List<ColumnMetaData> buildGetSuperTablesColumnMetaDataList() {
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(buildTableCatalogMeta(1));       // 1. TABLE_CAT
        columnMetaDataList.add(buildTableSchemaMeta(2));        // 2. TABLE_SCHEM
        columnMetaDataList.add(buildTableNameMeta(3));          // 3. TABLE_NAME
        columnMetaDataList.add(buildSuperTableNameMeta(4));     // 4. SUPERTABLE_NAME
        return columnMetaDataList;
    }

    private ColumnMetaData buildTableCatalogMeta(int colIndex) {
        ColumnMetaData col1 = new ColumnMetaData();
        col1.setColIndex(colIndex);
        col1.setColName("TABLE_CAT");
        col1.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col1;
    }

    private ColumnMetaData buildTableSchemaMeta(int colIndex) {
        ColumnMetaData col2 = new ColumnMetaData();
        col2.setColIndex(colIndex);
        col2.setColName("TABLE_SCHEM");
        col2.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col2;
    }

    private ColumnMetaData buildTableNameMeta(int colIndex) {
        ColumnMetaData col3 = new ColumnMetaData();
        col3.setColIndex(colIndex);
        col3.setColName("TABLE_NAME");
        col3.setColSize(193);
        col3.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col3;
    }

    private ColumnMetaData buildSuperTableNameMeta(int colIndex) {
        ColumnMetaData col4 = new ColumnMetaData();
        col4.setColIndex(colIndex);
        col4.setColName("SUPERTABLE_NAME");
        col4.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col4;
    }

}
