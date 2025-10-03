package com.github.jhonatas48.migrationapi;

import com.github.jhonatas48.migrationapi.core.ChangeLogApplier;
import com.github.jhonatas48.migrationapi.core.ChangeLogGenerator;
import com.github.jhonatas48.migrationapi.core.YamlConstraintPostProcessor;
import com.github.jhonatas48.migrationapi.core.datasource.DataSourceConnectionInfoProvider;
import com.github.jhonatas48.migrationapi.core.datasource.SpringDataSourceConnectionInfoProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;

/**
 * Auto configuração da Migration API.
 * - Expõe beans com dependências claras (SRP) e nomes descritivos.
 * - O ChangeLogGenerator usa o provider para pegar URL/credenciais do Spring e
 *   o YamlConstraintPostProcessor para nomear constraints quando necessário.
 * - O ChangeLogApplier trabalha diretamente com DataSource, pois precisa detectar
 *   SQLite e aplicar o “table rebuild” antes de rodar Liquibase no YAML sanitizado.
 */
@AutoConfiguration
@EnableConfigurationProperties(MigrationProperties.class)
public class MigrationAutoConfiguration {

    @Bean
    public DataSourceConnectionInfoProvider dataSourceConnectionInfoProvider(DataSourceProperties dataSourceProperties,
                                                                             DataSource dataSource) {
        return new SpringDataSourceConnectionInfoProvider(dataSourceProperties, dataSource);
    }

    @Bean
    public YamlConstraintPostProcessor yamlConstraintPostProcessor(MigrationProperties migrationProperties) {
        return new YamlConstraintPostProcessor(migrationProperties);
    }

    @Bean
    public ChangeLogGenerator changeLogGenerator(MigrationProperties migrationProperties,
                                                 DataSourceConnectionInfoProvider connectionInfoProvider,
                                                 YamlConstraintPostProcessor yamlConstraintPostProcessor) {
        return new ChangeLogGenerator(migrationProperties, connectionInfoProvider, yamlConstraintPostProcessor);
    }

    @Bean
    public ChangeLogApplier changeLogApplier(DataSource dataSource) {
        return new ChangeLogApplier(dataSource);
    }

    @Bean
    public MigrationService migrationService(MigrationProperties migrationProperties,
                                             ChangeLogGenerator changeLogGenerator,
                                             ChangeLogApplier changeLogApplier) {
        return new MigrationService(migrationProperties, changeLogGenerator, changeLogApplier);
    }

    @Bean
    public AutoMigrationRunner autoMigrationRunner(MigrationProperties migrationProperties,
                                                   MigrationService migrationService) {
        return new AutoMigrationRunner(migrationProperties, migrationService);
    }
}
