package com.github.jhonatas48.migrationapi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

public class AutoMigrationRunner implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(AutoMigrationRunner.class);

    private final MigrationProperties properties;
    private final MigrationService migrationService;

    public AutoMigrationRunner(MigrationProperties properties, MigrationService migrationService) {
        this.properties = properties;
        this.migrationService = migrationService;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (!properties.isEnabled()) {
            LOGGER.info("Liquibase Migration API: desabilitado via propriedade 'migrationapi.enabled=false'.");
            return;
        }

        LOGGER.info("Liquibase Migration API: generate-and-apply={} — executando...", properties.isGenerateAndApply());
        if (properties.isGenerateAndApply()) {
            migrationService.generateAndApply();
        } else {
            migrationService.generateDiffChangeLog();
        }
    }
}
