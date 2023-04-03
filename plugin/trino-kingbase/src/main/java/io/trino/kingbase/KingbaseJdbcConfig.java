package io.trino.kingbase;

import io.trino.plugin.jdbc.BaseJdbcConfig;

public class KingbaseJdbcConfig extends BaseJdbcConfig {
//    @AssertTrue(message = "Invalid JDBC URL for MySQL connector")
//    public boolean isUrlValid() {
//        try {
//            return new Driver().acceptsURL(getConnectionUrl());
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    @AssertTrue(message = "Database (catalog) must not be specified in JDBC URL for MySQL connector")
//    public boolean isUrlWithoutDatabase() {
//        try {
//            ConnectionUrlParser parser = parseConnectionString(getConnectionUrl());
//            return isNullOrEmpty(parser.getPath());
//        } catch (CJException ignore) {
//            return false;
//        }
//    }
}
