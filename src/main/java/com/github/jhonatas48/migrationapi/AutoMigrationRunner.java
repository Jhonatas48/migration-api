package com.github.jhonatas48.migrationapi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

@ConditionalOnProperty(prefix = "app.migrations", name = "run-at-startup", havingValue = "true")
public class AutoMigrationRunner implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(AutoMigrationRunner.class);
    private final MigrationService migrationService;

    public AutoMigrationRunner(MigrationService migrationService, MigrationProperties properties) {
        this.migrationService = migrationService;
    }

    @Override
    public void run(org.springframework.boot.ApplicationArguments args) throws Exception {
        LOGGER.info("Liquibase Migration API: run-at-startup=true — gerando e aplicando diffs (se houver)...");
        migrationService.generateAndApply();
    }
}
