package com.github.jhonatas48.migrationapi;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties(MigrationProperties.class)
@ConditionalOnClass(name = "liquibase.command.CommandScope")
public class MigrationAutoConfiguration {

    @Bean
    public MigrationService migrationService(MigrationProperties properties) {
        return new MigrationService(properties);
    }

    @Bean
    public AutoMigrationRunner autoMigrationRunner(MigrationService migrationService, MigrationProperties properties) {
        return new AutoMigrationRunner(migrationService, properties);
    }
}
