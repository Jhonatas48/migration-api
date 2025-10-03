package com.github.jhonatas48.migrationapi.core;

import com.github.jhonatas48.migrationapi.MigrationProperties;
import com.github.jhonatas48.migrationapi.core.datasource.DataSourceConnectionInfo;
import com.github.jhonatas48.migrationapi.core.datasource.DataSourceConnectionInfoProvider;
import liquibase.Scope;
import liquibase.command.CommandScope;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;

/**
 * Responsável por gerar o diff ChangeLog (Hibernate x Banco) usando Liquibase.
 * Depende apenas de MigrationProperties e do provider de conexão (SRP / DI / SOLID).
 */
public class ChangeLogGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeLogGenerator.class);

    private static final String LIQUIBASE_PLUGIN_PACKAGES_PROP = "liquibase.plugin.packages";
    private static final String HIBERNATE_PLUGIN_PACKAGE = "liquibase.ext.hibernate";
    private static final String CHANGE_LOG_FILE_PREFIX = "changelog-";
    private static final String CHANGE_LOG_FILE_SUFFIX = ".yaml";
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private final MigrationProperties migrationProperties;
    private final DataSourceConnectionInfoProvider connectionInfoProvider;
    private final YamlConstraintPostProcessor yamlConstraintPostProcessor; // se você já tem essa classe utilitária

    public ChangeLogGenerator(MigrationProperties migrationProperties,
                              DataSourceConnectionInfoProvider connectionInfoProvider,
                              YamlConstraintPostProcessor yamlConstraintPostProcessor) {
        this.migrationProperties = migrationProperties;
        this.connectionInfoProvider = connectionInfoProvider;
        this.yamlConstraintPostProcessor = yamlConstraintPostProcessor;
    }

    public GeneratedChangeLog generateDiffChangeLog() throws Exception {
        Path outputDirectory = Paths.get(migrationProperties.getOutputDir()).toAbsolutePath().normalize();
        Files.createDirectories(outputDirectory);

        Path changeLogPath = outputDirectory.resolve(
                CHANGE_LOG_FILE_PREFIX + LocalDateTime.now().format(TIMESTAMP_FORMAT) + CHANGE_LOG_FILE_SUFFIX
        );

        // Extensão Hibernate
        System.setProperty(LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE);

        // Monta referenceUrl a partir das entidades + dialect
        String entityPackagesSegment = buildEntityPackagesSegment(migrationProperties.getEntityPackage());
        String dialect = nullToEmpty(migrationProperties.getDialect()).trim();
        String referenceUrl = "hibernate:spring:" + entityPackagesSegment + "?dialect=" + dialect;

        // Pega URL/credenciais direto do Spring (erro claro se faltarem)
        DataSourceConnectionInfo connectionInfo = connectionInfoProvider.resolve();

        CommandScope diff = new CommandScope("diffChangeLog");
        diff.addArgumentValue("referenceUrl", referenceUrl);
        diff.addArgumentValue("changeLogFile", changeLogPath.toString());
        diff.addArgumentValue("url", connectionInfo.getJdbcUrl());
        putIfPresent(diff, "username", connectionInfo.getUsername());
        putIfPresent(diff, "password", connectionInfo.getPassword());

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        ResourceAccessor classpathAccessor = new ClassLoaderResourceAccessor(contextClassLoader);

        Map<String, Object> scope = Map.of(
                Scope.Attr.resourceAccessor.name(), classpathAccessor,
                Scope.Attr.classLoader.name(), contextClassLoader,
                LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE
        );

        Scope.child(scope, () -> { diff.execute(); return null; });

        String generatedYaml = Files.readString(changeLogPath, StandardCharsets.UTF_8);

        boolean hasChangeSets = generatedYaml.contains("\n- changeSet:");
        if (!hasChangeSets && migrationProperties.isSkipWhenEmpty()) {
            Files.deleteIfExists(changeLogPath);
            LOGGER.info("Nenhuma diferença detectada. Nada a aplicar.");
            return new GeneratedChangeLog(null, "", false);
        }

        String finalYaml = generatedYaml;

        if (migrationProperties.isAutoNameConstraints() && yamlConstraintPostProcessor != null) {
            String processed = yamlConstraintPostProcessor.process(finalYaml);
            if (!Objects.equals(finalYaml, processed)) {
                Files.writeString(changeLogPath, processed, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);
                LOGGER.info("YAML ajustado: constraints sem nome foram nomeadas automaticamente.");
                finalYaml = processed;
            }
        }

        if (migrationProperties.isLogDiffContent()) {
            LOGGER.info("YAML gerado:\n{}", finalYaml);
        }

        LOGGER.info("ChangeLog gerado em {}", changeLogPath);
        return new GeneratedChangeLog(changeLogPath.toString(), finalYaml, true);
    }

    private static void putIfPresent(CommandScope command, String argumentName, String value) {
        if (value != null && !value.isBlank()) {
            command.addArgumentValue(argumentName, value);
        }
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    private static String buildEntityPackagesSegment(String configuredPackages) {
        String source = (configuredPackages == null || configuredPackages.isBlank())
                ? "com.exemplo.domain"
                : configuredPackages;

        String[] split = source.split("[,;]");
        StringBuilder joined = new StringBuilder();
        for (String s : split) {
            String pkg = s.trim();
            if (!pkg.isEmpty()) {
                if (!joined.isEmpty()) joined.append(",");
                joined.append(pkg);
            }
        }
        return joined.isEmpty() ? "com.exemplo.domain" : joined.toString();
    }

    /** DTO interno equivalente ao que o MigrationService já espera. */
    public static final class GeneratedChangeLog {
        private final String absolutePath;
        private final String content;
        private final boolean hasActionableChanges;

        public GeneratedChangeLog(String absolutePath, String content, boolean hasActionableChanges) {
            this.absolutePath = absolutePath;
            this.content = content;
            this.hasActionableChanges = hasActionableChanges;
        }

        public String getAbsolutePath() {
            return absolutePath;
        }

        public String getContent() {
            return content;
        }

        public boolean hasActionableChanges() {
            return hasActionableChanges;
        }
    }
}
