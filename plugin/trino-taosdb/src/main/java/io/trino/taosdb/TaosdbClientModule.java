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

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.taosdata.jdbc.rs.RestfulDriver;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.*;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.ptf.ConnectorTableFunction;

import java.sql.SQLException;
import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class TaosdbClientModule extends AbstractConfigurationAwareModule {
    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider
            , TaosdbConfig taosdbConfig)
            throws SQLException {
        return new DriverConnectionFactory(
                new RestfulDriver(),
                config.getConnectionUrl(),
                getConnectionProperties(taosdbConfig),
                credentialProvider);
    }

    public static Properties getConnectionProperties(TaosdbConfig config) {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("connectionTimeZone", "Asia/Shanghai");
        //TODO 可以查看官方文档 进行属性配置
        return connectionProperties;
    }

    @Override
    protected void setup(Binder binder) {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(TaosdbClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(TaosdbJdbcConfig.class);
        configBinder(binder).bindConfig(TaosdbConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        install(new DecimalModule());
        install(new JdbcJoinPushdownSupportModule());
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }
}
