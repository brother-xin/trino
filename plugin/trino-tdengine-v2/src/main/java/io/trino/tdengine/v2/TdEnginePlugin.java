package io.trino.tdengine.v2;

import io.trino.plugin.jdbc.JdbcPlugin;

public class TdEnginePlugin extends JdbcPlugin {
    public TdEnginePlugin() {
        super("taosdb", new TdEngineClientModule());
    }
}
