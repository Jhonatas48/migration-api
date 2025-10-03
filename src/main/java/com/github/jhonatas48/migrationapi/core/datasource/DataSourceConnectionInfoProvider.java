package com.github.jhonatas48.migrationapi.core.datasource;

/**
 * Fornece de forma consistente os detalhes de conexão do DataSource gerenciado pelo Spring.
 */
public interface DataSourceConnectionInfoProvider {

    /**
     * Resolve a URL JDBC, usuário e senha do DataSource do Spring.
     * @return informações de conexão (a URL é obrigatória).
     * @throws IllegalStateException quando a URL não puder ser determinada.
     */
    DataSourceConnectionInfo resolve();
}
