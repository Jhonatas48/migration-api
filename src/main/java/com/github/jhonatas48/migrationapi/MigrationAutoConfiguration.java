package com.github.jhonatas48.migrationapi;

import com.github.jhonatas48.migrationapi.core.ChangeLogApplier;
import com.github.jhonatas48.migrationapi.core.ChangeLogGenerator;
import com.github.jhonatas48.migrationapi.core.datasource.DataSourceConnectionInfoProvider;
import com.github.jhonatas48.migrationapi.core.datasource.SpringDataSourceConnectionInfoProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;

@AutoConfiguration
@EnableConfigurationProperties(MigrationProperties.class)
public class MigrationAutoConfiguration {

    @Bean
    public DataSourceConnectionInfoProvider dataSourceConnectionInfoProvider(DataSourceProperties dataSourceProperties,
                                                                             DataSource dataSource) {
        return new SpringDataSourceConnectionInfoProvider(dataSourceProperties, dataSource);
    }

    @Bean
    public ChangeLogGenerator changeLogGenerator(MigrationProperties properties,
                                                 DataSourceConnectionInfoProvider connectionInfoProvider) {
        // Se você tiver a classe utilitária de pós-processamento, injete-a aqui. Caso não tenha, passe null.
        return new ChangeLogGenerator(properties, connectionInfoProvider, /* yamlConstraintPostProcessor */ null);
    }

    @Bean
    public ChangeLogApplier changeLogApplier(DataSourceConnectionInfoProvider connectionInfoProvider) {
        return new ChangeLogApplier(connectionInfoProvider);
    }

    @Bean
    public MigrationService migrationService(MigrationProperties properties,
                                             ChangeLogGenerator changeLogGenerator,
                                             ChangeLogApplier changeLogApplier) {
        return new MigrationService(properties, changeLogGenerator, changeLogApplier);
    }

    @Bean
    public AutoMigrationRunner autoMigrationRunner(MigrationProperties properties,
                                                   MigrationService migrationService) {
        return new AutoMigrationRunner(properties, migrationService);
    }
}
