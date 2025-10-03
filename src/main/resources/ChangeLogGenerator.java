
package com.github.jhonatas48.migrationapi.core;

import com.github.jhonatas48.migrationapi.MigrationProperties;
import com.github.jhonatas48.migrationapi.MigrationService;
import com.github.jhonatas48.migrationapi.support.HibernateReferenceUrlBuilder;
import liquibase.Scope;
import liquibase.command.CommandScope;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;

public class ChangeLogGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeLogGenerator.class);

    private static final String LIQUIBASE_PLUGIN_PACKAGES_PROP = "liquibase.plugin.packages";
    private static final String HIBERNATE_PLUGIN_PACKAGE = "liquibase.ext.hibernate";
    private static final String CHANGE_LOG_FILE_PREFIX = "changelog-";
    private static final String CHANGE_LOG_FILE_SUFFIX = ".yaml";
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private final MigrationProperties migrationProperties;
    private final HibernateReferenceUrlBuilder hibernateReferenceUrlBuilder;
    private final YamlConstraintPostProcessor yamlConstraintPostProcessor;
    private final SqliteChangeSetRewriter sqliteChangeSetRewriter;
    private final EnumConstraintStrategy enumConstraintStrategy;

    public ChangeLogGenerator(MigrationProperties migrationProperties,
                              HibernateReferenceUrlBuilder hibernateReferenceUrlBuilder,
                              YamlConstraintPostProcessor yamlConstraintPostProcessor,
                              SqliteChangeSetRewriter sqliteChangeSetRewriter,
                              EnumConstraintStrategy enumConstraintStrategy) {
        this.migrationProperties = migrationProperties;
        this.hibernateReferenceUrlBuilder = hibernateReferenceUrlBuilder;
        this.yamlConstraintPostProcessor = yamlConstraintPostProcessor;
        this.sqliteChangeSetRewriter = sqliteChangeSetRewriter;
        this.enumConstraintStrategy = enumConstraintStrategy;
    }

    public MigrationService.MigrationDiffResult generateDiffChangeLog() throws Exception {
        Path outputDirectory = Paths.get(migrationProperties.getOutputDir()).toAbsolutePath().normalize();
        Files.createDirectories(outputDirectory);
        Path changeLogPath = outputDirectory.resolve(CHANGE_LOG_FILE_PREFIX + LocalDateTime.now().format(TIMESTAMP_FORMAT) + CHANGE_LOG_FILE_SUFFIX);

        System.setProperty(LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE);

        CommandScope diff = new CommandScope("diffChangeLog");
        diff.addArgumentValue("referenceUrl", hibernateReferenceUrlBuilder.buildReferenceUrl(migrationProperties));
        diff.addArgumentValue("changeLogFile", changeLogPath.toString());
        diff.addArgumentValue("url", System.getProperty("spring.datasource.url"));
        putIfPresent(diff, "username", System.getProperty("spring.datasource.username"));
        putIfPresent(diff, "password", System.getProperty("spring.datasource.password"));

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        ResourceAccessor classpathAccessor = new ClassLoaderResourceAccessor(contextClassLoader);

        Map<String, Object> scope = Map.of(
                Scope.Attr.resourceAccessor.name(), classpathAccessor,
                Scope.Attr.classLoader.name(), contextClassLoader,
                LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE
        );

        Scope.child(scope, () -> { diff.execute(); return null; });

        String generatedYaml = Files.readString(changeLogPath, StandardCharsets.UTF_8);

        boolean hasChangeSets = generatedYaml.contains("\n- changeSet:") || generatedYaml.contains("
- changeSet:");
        if (!hasChangeSets && migrationProperties.isSkipWhenEmpty()) {
            Files.deleteIfExists(changeLogPath);
            LOGGER.info("Nenhuma diferença detectada. Nada a aplicar.");
            return new MigrationService.MigrationDiffResult(null, "", false);
        }

        String processedYaml = generatedYaml;
        processedYaml = yamlConstraintPostProcessor.process(processedYaml);
        if (migrationProperties.isSqliteRewriteEnabled()) {
            processedYaml = sqliteChangeSetRewriter.rewrite(processedYaml);
        }
        processedYaml = enumConstraintStrategy.process(processedYaml);

        if (!Objects.equals(generatedYaml, processedYaml)) {
            Files.writeString(changeLogPath, processedYaml, StandardCharsets.UTF_8);
            LOGGER.info("YAML pós-processado e salvo.");
        }

        if (migrationProperties.isLogDiffContent()) {
            LOGGER.info("YAML final:\n{}", processedYaml);
        }

        LOGGER.info("ChangeLog gerado em {}", changeLogPath);
        return new MigrationService.MigrationDiffResult(changeLogPath.toString(), processedYaml, hasChangeSets);
    }

    private static void putIfPresent(CommandScope command, String name, String value) {
        if (value != null && !value.isBlank()) {
            command.addArgumentValue(name, value);
        }
    }
}
