package com.github.jhonatas48.migrationapi.core.datasource;

import java.util.Objects;

/**
 * Valor imutável com as informações de conexão JDBC necessárias para o Liquibase.
 */
public final class DataSourceConnectionInfo {

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public DataSourceConnectionInfo(String jdbcUrl, String username, String password) {
        this.jdbcUrl = Objects.requireNonNull(jdbcUrl, "jdbcUrl não pode ser nulo");
        this.username = username;
        this.password = password;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
