package io.trino.tdengine.v2;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

/**
 * connector.name=tdengine
 * connection.url=jdbc:TAOS-RS://192.168.11.91:30003?user=root&password=taosdata
 * connection.user=root
 * connection.password=taosdata
 */
public class TdEngineConfig {
    private String url;
    private String user;
    private String password;


    @Config("connection.url")
    @ConfigDescription("Connection Url required to access the TdEngine")
    @ConfigSecuritySensitive
    public TdEngineConfig setUrl(String url) {
        this.url = url;
        return this;
    }

    @Config("connection.user")
    @ConfigDescription("Connection User required to access the TdEngine")
    @ConfigSecuritySensitive
    public TdEngineConfig setUser(String user) {
        this.user = user;
        return this;
    }

    @Config("connection.password")
    @ConfigDescription("Connection Password required to access the TdEngine")
    @ConfigSecuritySensitive
    public TdEngineConfig setPassword(String password) {
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
