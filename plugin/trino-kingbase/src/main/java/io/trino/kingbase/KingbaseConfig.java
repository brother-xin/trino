package io.trino.kingbase;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

public class KingbaseConfig {
    private String url;
    private String user;
    private String password;


    @Config("connection.url")
    @ConfigDescription("Connection Url required to access the TdEngine")
    @ConfigSecuritySensitive
    public KingbaseConfig setUrl(String url) {
        this.url = url;
        return this;
    }

    @Config("connection.user")
    @ConfigDescription("Connection User required to access the TdEngine")
    @ConfigSecuritySensitive
    public KingbaseConfig setUser(String user) {
        this.user = user;
        return this;
    }

    @Config("connection.password")
    @ConfigDescription("Connection Password required to access the TdEngine")
    @ConfigSecuritySensitive
    public KingbaseConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
