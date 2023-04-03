package io.trino.kingbase;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.kingbase8.Driver;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.*;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.ptf.ConnectorTableFunction;

import java.sql.SQLException;
import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class KingbaseClientModule extends AbstractConfigurationAwareModule {
    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider
            , KingbaseConfig kingBaseConfig)
            throws SQLException {
        return new DriverConnectionFactory(
                new Driver(),
                config.getConnectionUrl(),
                getConnectionProperties(kingBaseConfig),
                credentialProvider);
    }

    public static Properties getConnectionProperties(KingbaseConfig config) {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("connectionTimeZone", "Asia/Shanghai");
        //TODO 可以查看官方文档 进行属性配置
        return connectionProperties;
    }

    @Override
    protected void setup(Binder binder) {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(KingbaseClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(KingbaseJdbcConfig.class);
        configBinder(binder).bindConfig(KingbaseConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        install(new DecimalModule());
        install(new JdbcJoinPushdownSupportModule());
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }
}
