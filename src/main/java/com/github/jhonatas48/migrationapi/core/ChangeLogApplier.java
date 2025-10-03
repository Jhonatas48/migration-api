package com.github.jhonatas48.migrationapi.core;

import com.github.jhonatas48.migrationapi.core.datasource.DataSourceConnectionInfo;
import com.github.jhonatas48.migrationapi.core.datasource.DataSourceConnectionInfoProvider;
import liquibase.Scope;
import liquibase.command.CommandScope;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.CompositeResourceAccessor;
import liquibase.resource.DirectoryResourceAccessor;
import liquibase.resource.ResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Aplica um changelog existente usando Liquibase.
 */
public class ChangeLogApplier {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeLogApplier.class);
    private static final String LIQUIBASE_PLUGIN_PACKAGES_PROP = "liquibase.plugin.packages";
    private static final String HIBERNATE_PLUGIN_PACKAGE = "liquibase.ext.hibernate";

    private final DataSourceConnectionInfoProvider connectionInfoProvider;

    public ChangeLogApplier(DataSourceConnectionInfoProvider connectionInfoProvider) {
        this.connectionInfoProvider = connectionInfoProvider;
    }

    public void applyChangeLog(String changeLogAbsolutePath) throws Exception {
        if (changeLogAbsolutePath == null || changeLogAbsolutePath.isBlank()) {
            LOGGER.info("applyChangeLog chamado sem arquivo. Ignorando.");
            return;
        }

        Path path = Paths.get(changeLogAbsolutePath).toAbsolutePath().normalize();
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("Arquivo não encontrado: " + path);
        }

        System.setProperty(LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE);

        DataSourceConnectionInfo connectionInfo = connectionInfoProvider.resolve();

        CommandScope update = new CommandScope("update");
        update.addArgumentValue("changelogFile", path.getFileName().toString());
        update.addArgumentValue("url", connectionInfo.getJdbcUrl());
        putIfPresent(update, "username", connectionInfo.getUsername());
        putIfPresent(update, "password", connectionInfo.getPassword());

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        ResourceAccessor classpathAccessor = new ClassLoaderResourceAccessor(contextClassLoader);
        ResourceAccessor directoryAccessor = new DirectoryResourceAccessor(path.getParent());
        ResourceAccessor compositeAccessor = new CompositeResourceAccessor(directoryAccessor, classpathAccessor);

        Map<String, Object> scope = Map.of(
                Scope.Attr.resourceAccessor.name(), compositeAccessor,
                Scope.Attr.classLoader.name(), contextClassLoader,
                LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE
        );

        Scope.child(scope, () -> { update.execute(); return null; });

        LOGGER.info("ChangeLog aplicado: {}", path);
    }

    private static void putIfPresent(CommandScope command, String argumentName, String value) {
        if (value != null && !value.isBlank()) {
            command.addArgumentValue(argumentName, value);
        }
    }
}
