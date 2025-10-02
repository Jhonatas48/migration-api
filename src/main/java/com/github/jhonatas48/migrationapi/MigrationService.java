package com.github.jhonatas48.migrationapi;

import liquibase.Scope;
import liquibase.command.CommandScope;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.CompositeResourceAccessor;
import liquibase.resource.DirectoryResourceAccessor;
import liquibase.resource.ResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;

public class MigrationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationService.class);
    private final MigrationProperties properties;

    @Value("${spring.datasource.url}")
    private String dataSourceUrl;

    @Value("${spring.datasource.username:}")
    private String dataSourceUsername;

    @Value("${spring.datasource.password:}")
    private String dataSourcePassword;

    private static final String ENTITY_PACKAGE = "com.exemplo.domain";
    private static final String HIBERNATE_PLUGIN_PACKAGE = "liquibase.ext.hibernate";
    private static final String LIQUIBASE_PLUGIN_PACKAGES_PROP = "liquibase.plugin.packages";
    private static final String CHANGE_LOG_FILE_PREFIX = "changelog-";
    private static final String CHANGE_LOG_FILE_SUFFIX = ".yaml";
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private static final Pattern YAML_CHANGESET_PATTERN = Pattern.compile("(?m)^\s*-\s*changeSet\s*:");

    public MigrationService(MigrationProperties properties) {
        this.properties = properties;
    }

    public static final class MigrationDiffResult {
        private final String changeLogAbsolutePath;
        private final String changeLogContent;
        private final boolean containsActionableChanges;

        public MigrationDiffResult(String path, String content, boolean actionable) {
            this.changeLogAbsolutePath = path;
            this.changeLogContent = content;
            this.containsActionableChanges = actionable;
        }
        public String getChangeLogAbsolutePath() { return changeLogAbsolutePath; }
        public String getChangeLogContent() { return changeLogContent; }
        public boolean containsActionableChanges() { return containsActionableChanges; }
    }

    public void generateAndApply() throws Exception {
        MigrationDiffResult result = generateDiffChangeLog();
        applyChangeLog(result);
    }

    public MigrationDiffResult generateDiffChangeLog() throws Exception {
        Path outputDirectory = Paths.get(properties.getOutputDir()).toAbsolutePath().normalize();
        Files.createDirectories(outputDirectory);
        Path changeLogPath = outputDirectory.resolve(CHANGE_LOG_FILE_PREFIX + LocalDateTime.now().format(TIMESTAMP_FORMAT) + CHANGE_LOG_FILE_SUFFIX);

        System.setProperty(LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE);
        String referenceUrl = "hibernate:spring:" + ENTITY_PACKAGE + "?dialect=" + properties.getDialect();

        CommandScope diff = new CommandScope("diffChangeLog");
        diff.addArgumentValue("referenceUrl", referenceUrl);
        diff.addArgumentValue("changeLogFile", changeLogPath.toString());
        diff.addArgumentValue("url", dataSourceUrl);

        if (dataSourceUsername != null && !dataSourceUsername.isBlank()) {
            diff.addArgumentValue("username", dataSourceUsername);
        }
        if (dataSourcePassword != null && !dataSourcePassword.isBlank()) {
            diff.addArgumentValue("password", dataSourcePassword);
        }

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        ResourceAccessor accessor = new ClassLoaderResourceAccessor(cl);
        Map<String, Object> scope = Map.of(Scope.Attr.resourceAccessor.name(), accessor, Scope.Attr.classLoader.name(), cl, LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE);

        Scope.child(scope, () -> { diff.execute(); return null; });

        String yaml = Files.readString(changeLogPath, StandardCharsets.UTF_8);
        boolean actionable = YAML_CHANGESET_PATTERN.matcher(yaml).find();
        if (!actionable) {
            Files.deleteIfExists(changeLogPath);
            LOGGER.info("Nenhuma diferença detectada. Nada a aplicar.");
            return new MigrationDiffResult(null, "", false);
        }

        LOGGER.info("ChangeLog gerado em {}", changeLogPath);
        return new MigrationDiffResult(changeLogPath.toString(), yaml, true);
    }

    public void applyChangeLog(MigrationDiffResult result) throws Exception {
        if (result == null || !result.containsActionableChanges() || result.getChangeLogAbsolutePath() == null) {
            LOGGER.info("Sem mudanças — aplicação ignorada.");
            return;
        }
        applyChangeLog(result.getChangeLogAbsolutePath());
    }

    public void applyChangeLog(String changeLogAbsolutePath) throws Exception {
        if (changeLogAbsolutePath == null || changeLogAbsolutePath.isBlank()) {
            LOGGER.info("applyChangeLog chamado sem arquivo. Ignorando.");
            return;
        }

        Path path = Paths.get(changeLogAbsolutePath).toAbsolutePath().normalize();
        if (!Files.exists(path)) throw new IOException("Arquivo não encontrado: " + path);

        System.setProperty(LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE);

        CommandScope update = new CommandScope("update");
        update.addArgumentValue("changelogFile", path.getFileName().toString());
        update.addArgumentValue("url", dataSourceUrl);

        if (dataSourceUsername != null && !dataSourceUsername.isBlank()) {
            update.addArgumentValue("username", dataSourceUsername);
        }
        if (dataSourcePassword != null && !dataSourcePassword.isBlank()) {
            update.addArgumentValue("password", dataSourcePassword);
        }

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        ResourceAccessor classpath = new ClassLoaderResourceAccessor(cl);
        ResourceAccessor dir = new DirectoryResourceAccessor(path.getParent());
        ResourceAccessor composite = new CompositeResourceAccessor(dir, classpath);

        Map<String, Object> scope = Map.of(Scope.Attr.resourceAccessor.name(), composite, Scope.Attr.classLoader.name(), cl, LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE);
        Scope.child(scope, () -> { update.execute(); return null; });

        LOGGER.info("ChangeLog aplicado: {}", path);
    }
}
