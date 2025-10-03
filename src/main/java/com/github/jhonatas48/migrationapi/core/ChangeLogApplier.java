package com.github.jhonatas48.migrationapi.core;

import com.github.jhonatas48.migrationapi.core.audit.MigrationAuditService;
import liquibase.Scope;
import liquibase.command.CommandScope;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.CompositeResourceAccessor;
import liquibase.resource.DirectoryResourceAccessor;
import liquibase.resource.ResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Map;
import java.util.Objects;

public class ChangeLogApplier {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeLogApplier.class);

    private static final String LIQUIBASE_PLUGIN_PACKAGES_PROP = "liquibase.plugin.packages";
    private static final String HIBERNATE_PLUGIN_PACKAGE = "liquibase.ext.hibernate";

    private final DataSource dataSource;

    public ChangeLogApplier(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void applyChangeLog(String changeLogAbsolutePath) throws Exception {
        Path path = Paths.get(changeLogAbsolutePath).toAbsolutePath().normalize();
        if (!Files.exists(path)) {
            throw new NoSuchFileException("Arquivo não encontrado: " + path);
        }

        // Carrega YAML original
        String originalYaml = Files.readString(path, StandardCharsets.UTF_8);

        // 1) Sanitiza para SQLite: remove changes de FK e aplica rebuilds
        MigrationAuditService auditService = new MigrationAuditService(dataSource);
        SqliteForeignKeySanitizer sqliteSanitizer = new SqliteForeignKeySanitizer(dataSource, auditService);

        SqliteForeignKeySanitizer.SanitizationResult result = sqliteSanitizer.sanitizeAndApplyRebuilds(originalYaml);
        String yamlToApply = result.getYamlForLiquibase();

        // Se alterou, escreve um arquivo "-sqlite" ao lado, para histórico e leitura pelo Liquibase
        Path toApplyPath = path;
        if (result.wasAltered()) {
            Path sqlitePath = path.getParent().resolve(path.getFileName().toString().replace(".yaml", "-sqlite.yaml"));
            Files.writeString(sqlitePath, yamlToApply, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            LOGGER.info("YAML ajustado para SQLite gravado em {}", sqlitePath);
            toApplyPath = sqlitePath;
        }

        // 2) Rodar Liquibase com o arquivo final
        System.setProperty(LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE);

        CommandScope update = new CommandScope("update");
        update.addArgumentValue("changelogFile", toApplyPath.getFileName().toString());

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        ResourceAccessor classpathAccessor = new ClassLoaderResourceAccessor(cl);
        ResourceAccessor directoryAccessor = new DirectoryResourceAccessor(toApplyPath.getParent());
        ResourceAccessor compositeAccessor = new CompositeResourceAccessor(directoryAccessor, classpathAccessor);

        Map<String, Object> scope = Map.of(
                Scope.Attr.resourceAccessor.name(), compositeAccessor,
                Scope.Attr.classLoader.name(), cl,
                LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE
        );

        Scope.child(scope, () -> { update.execute(); return null; });

        LOGGER.info("ChangeLog aplicado: {}", toApplyPath);
    }
}
