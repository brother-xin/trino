package io.trino.tdengine;

import com.google.inject.Module;
import com.google.inject.*;
import com.taosdata.jdbc.rs.RestfulDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class TdEngineModule implements Module {
    public TdEngineModule() {
    }

    @Provides
    @Singleton
    public static Connection createConnectionFactory(TdEngineConfig config)
            throws SQLException {
        DriverManager.registerDriver(new RestfulDriver());
        Connection connection = DriverManager.getConnection(config.getUrl());
        TdEngineRecordCursor.connection = connection;
        return connection;
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(TdEngineConnector.class).in(Scopes.SINGLETON);
        binder.bind(TdEngineConnectorMetadata.class).in(Scopes.SINGLETON);
        binder.bind(TdEngineClient.class).in(Scopes.SINGLETON);
        binder.bind(TdEngineSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(TdEngineRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(TdEngineConfig.class);
    }
}
