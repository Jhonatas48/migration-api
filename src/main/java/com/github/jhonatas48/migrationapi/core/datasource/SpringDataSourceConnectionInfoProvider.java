package com.github.jhonatas48.migrationapi.core.datasource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;

/**
 * Implementação que extrai a URL (e, quando disponível, usuário/senha) do contexto Spring.
 * Regra:
 *   1) Usa DataSourceProperties.getUrl() se disponível;
 *   2) Caso contrário, tenta DataSource#getConnection().getMetaData().getURL();
 *   3) Se não achar, lança IllegalStateException com uma mensagem clara.
 */
public class SpringDataSourceConnectionInfoProvider implements DataSourceConnectionInfoProvider {

    private final DataSourceProperties dataSourceProperties;
    private final DataSource dataSource;

    public SpringDataSourceConnectionInfoProvider(DataSourceProperties dataSourceProperties,
                                                  DataSource dataSource) {
        this.dataSourceProperties = dataSourceProperties;
        this.dataSource = dataSource;
    }

    @Override
    public DataSourceConnectionInfo resolve() {
        String jdbcUrl = (dataSourceProperties != null) ? dataSourceProperties.getUrl() : null;

        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            jdbcUrl = tryResolveUrlFromDataSource();
        }

        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalStateException(
                    "Liquibase Migration API: não foi possível determinar a URL JDBC. " +
                            "Defina 'spring.datasource.url' ou use um DataSource que exponha a URL via metadata."
            );
        }

        // Para SQLite (ou outros), usuário/senha podem ser nulos/irrelevantes.
        String username = (dataSourceProperties != null) ? dataSourceProperties.getUsername() : null;
        String password = (dataSourceProperties != null) ? dataSourceProperties.getPassword() : null;

        return new DataSourceConnectionInfo(jdbcUrl, username, password);
    }

    private String tryResolveUrlFromDataSource() {
        if (dataSource == null) return null;
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            return (metaData != null) ? metaData.getURL() : null;
        } catch (Exception ignored) {
            return null;
        }
    }
}
