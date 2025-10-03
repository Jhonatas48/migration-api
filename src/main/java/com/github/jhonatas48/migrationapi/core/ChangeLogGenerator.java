package com.github.jhonatas48.migrationapi.core;

import com.github.jhonatas48.migrationapi.MigrationProperties;
import liquibase.Scope;
import liquibase.command.CommandScope;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class ChangeLogGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeLogGenerator.class);

    private static final String LIQUIBASE_PLUGIN_PACKAGES_PROP = "liquibase.plugin.packages";
    private static final String HIBERNATE_PLUGIN_PACKAGE = "liquibase.ext.hibernate";
    private static final String CHANGE_LOG_FILE_PREFIX = "changelog-";
    private static final String CHANGE_LOG_FILE_SUFFIX = ".yaml";
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private final MigrationProperties migrationProperties;
    private final DataSourceProperties dataSourceProperties;

    public ChangeLogGenerator(MigrationProperties migrationProperties,
                              DataSourceProperties dataSourceProperties) {
        this.migrationProperties = migrationProperties;
        this.dataSourceProperties = dataSourceProperties;
    }

    public GeneratedChangeLog generateDiffChangeLog() throws Exception {
        // 1) Garantir que temos a URL do JDBC
        String jdbcUrl = dataSourceProperties.getUrl();
        if (!StringUtils.hasText(jdbcUrl)) {
            throw new IllegalStateException(
                    "Liquibase: 'spring.datasource.url' não está definido. " +
                            "Defina a URL do JDBC para gerar o diffChangeLog.");
        }

        // 2) Preparar arquivo de saída
        Path outputDirectory = Path.of(migrationProperties.getOutputDir()).toAbsolutePath().normalize();
        Files.createDirectories(outputDirectory);

        Path changeLogPath = outputDirectory.resolve(
                CHANGE_LOG_FILE_PREFIX + LocalDateTime.now().format(TIMESTAMP_FORMAT) + CHANGE_LOG_FILE_SUFFIX
        );

        // 3) Configurar Liquibase + Extensão Hibernate
        System.setProperty(LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE);

        String dialect = StringUtils.hasText(migrationProperties.getDialect())
                ? migrationProperties.getDialect().trim()
                : "";

        String entityPackages = buildEntityPackagesSegment(migrationProperties.getEntityPackage());
        String referenceUrl = "hibernate:spring:" + entityPackages + "?dialect=" + dialect;

        CommandScope diff = new CommandScope("diffChangeLog");
        diff.addArgumentValue("referenceUrl", referenceUrl);
        diff.addArgumentValue("changeLogFile", changeLogPath.toString());

        // AQUI está a correção: passamos a URL (e credenciais se houverem)
        diff.addArgumentValue("url", jdbcUrl);
        if (StringUtils.hasText(dataSourceProperties.getUsername())) {
            diff.addArgumentValue("username", dataSourceProperties.getUsername());
        }
        if (StringUtils.hasText(dataSourceProperties.getPassword())) {
            diff.addArgumentValue("password", dataSourceProperties.getPassword());
        }

        // 4) ResourceAccessor e Scope
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor(contextClassLoader);
        Map<String, Object> scope = Map.of(
                Scope.Attr.resourceAccessor.name(), resourceAccessor,
                Scope.Attr.classLoader.name(), contextClassLoader,
                LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE
        );

        // 5) Executar o diff
        Scope.child(scope, () -> { diff.execute(); return null; });

        String rawYaml = Files.readString(changeLogPath, StandardCharsets.UTF_8);
        boolean hasChangeSets = rawYaml.contains("\n- changeSet:");

        LOGGER.info("ChangeLog gerado em {}", changeLogPath);
        return new GeneratedChangeLog(changeLogPath, rawYaml, hasChangeSets);
    }

    private static String buildEntityPackagesSegment(String configuredPackages) {
        String source = (configuredPackages == null || configuredPackages.isBlank())
                ? "com.exemplo.domain"
                : configuredPackages;

        String[] split = source.split("[,;]");
        StringBuilder builder = new StringBuilder();
        for (String s : split) {
            String trimmed = s.trim();
            if (!trimmed.isEmpty()) {
                if (builder.length() > 0) builder.append(',');
                builder.append(trimmed);
            }
        }
        return builder.toString();
    }

    // DTO simples usado pelo serviço
    public static final class GeneratedChangeLog {
        private final Path absolutePath;
        private final String yamlContent;
        private final boolean containsChangeSets;

        public GeneratedChangeLog(Path absolutePath, String yamlContent, boolean containsChangeSets) {
            this.absolutePath = absolutePath;
            this.yamlContent = yamlContent;
            this.containsChangeSets = containsChangeSets;
        }
        public Path getAbsolutePath() { return absolutePath; }
        public String getYamlContent() { return yamlContent; }
        public boolean isContainsChangeSets() { return containsChangeSets; }
    }
}
