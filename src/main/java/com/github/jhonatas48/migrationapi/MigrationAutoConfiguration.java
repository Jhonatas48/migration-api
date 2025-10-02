package com.github.jhonatas48.migrationapi;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuração da Migration API.
 *
 * - Habilita o binding de MigrationProperties (application.yml / properties)
 * - Expõe MigrationService como bean
 * - Registra o AutoMigrationRunner (opcional, governado por migrationapi.enabled)
 */
@AutoConfiguration
@EnableConfigurationProperties(MigrationProperties.class)
public class MigrationAutoConfiguration {

    @Bean
    public MigrationService migrationService(MigrationProperties properties) {
        return new MigrationService(properties);
    }

    @Bean
    public AutoMigrationRunner autoMigrationRunner(MigrationProperties properties,
                                                   MigrationService migrationService) {
        return new AutoMigrationRunner(properties, migrationService);
    }
}
