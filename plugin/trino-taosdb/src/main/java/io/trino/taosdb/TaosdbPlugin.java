package io.trino.taosdb;

import io.trino.plugin.jdbc.JdbcPlugin;

public class TaosdbPlugin extends JdbcPlugin {
    public TaosdbPlugin() {
        super("taosdb", new TaosdbClientModule());
    }
}
