package io.trino.kingbase;

import io.trino.plugin.jdbc.JdbcPlugin;

public class KingbasePlugin extends JdbcPlugin {
    public KingbasePlugin() {
        super("kingbase", new KingbaseClientModule());
    }
}
