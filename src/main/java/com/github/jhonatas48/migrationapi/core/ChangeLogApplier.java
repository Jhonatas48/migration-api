package com.github.jhonatas48.migrationapi.core;

import com.github.jhonatas48.migrationapi.core.audit.MigrationAuditService;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.CompositeResourceAccessor;
import liquibase.resource.DirectoryResourceAccessor;
import liquibase.resource.ResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.Statement;

public class ChangeLogApplier {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeLogApplier.class);

    private static final String LIQUIBASE_PLUGIN_PACKAGES_PROP = "liquibase.plugin.packages";
    private static final String HIBERNATE_PLUGIN_PACKAGE = "liquibase.ext.hibernate";

    private final DataSource dataSource;

    public ChangeLogApplier(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Aplica um changelog YAML (caminho absoluto). O YAML é sanitizado para SQLite
     * (rebuilds/FKs inline), opcionalmente gravado como "-sqlite.yaml" e então aplicado
     * via API Java do Liquibase usando a Connection do DataSource (sem CommandScope e sem 'url').
     */
    public void applyChangeLog(String changeLogAbsolutePath) throws Exception {
        Path path = Paths.get(changeLogAbsolutePath).toAbsolutePath().normalize();
        if (!Files.exists(path)) {
            throw new NoSuchFileException("Arquivo não encontrado: " + path);
        }

        // Lê YAML original
        String originalYaml = Files.readString(path, StandardCharsets.UTF_8);

        // 1) Sanitiza para SQLite: remove changes de FK e aplica rebuilds
        MigrationAuditService auditService = new MigrationAuditService(dataSource);
        SqliteForeignKeySanitizer sqliteSanitizer = new SqliteForeignKeySanitizer(dataSource, auditService);

        SqliteForeignKeySanitizer.SanitizationResult result = sqliteSanitizer.sanitizeAndApplyRebuilds(originalYaml);
        String yamlToApply = result.getYamlForLiquibase();

        // Se alterou, grava um "-sqlite.yaml" ao lado e vamos aplicar esse
        Path toApplyPath = path;
        if (result.wasAltered()) {
            Path sqlitePath = path.getParent().resolve(
                    path.getFileName().toString().replaceFirst("\\.ya?ml$", "") + "-sqlite.yaml"
            );
            Files.writeString(sqlitePath, yamlToApply, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            LOGGER.info("YAML ajustado para SQLite gravado em {}", sqlitePath);
            toApplyPath = sqlitePath;
        }

        // 2) Executa via API Java do Liquibase (sem CommandScope e sem 'url')
        System.setProperty(LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE);

        // Acessor que enxerga o diretório do changelog e o classpath (para includes)
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        ResourceAccessor classpathAccessor = new ClassLoaderResourceAccessor(cl);
        ResourceAccessor directoryAccessor = new DirectoryResourceAccessor(toApplyPath.getParent());
        ResourceAccessor compositeAccessor = new CompositeResourceAccessor(directoryAccessor, classpathAccessor);

        // Importante: o Liquibase vai procurar o arquivo pelo nome relativo ao 'directoryAccessor'
        String changeLogRelative = toApplyPath.getFileName().toString();

        try (Connection conn = dataSource.getConnection()) {
            // Em SQLite, garanta FK enforcement nesta conexão usada pelo Liquibase
            try (Statement st = conn.createStatement()) {
                st.execute("PRAGMA foreign_keys = ON");
            }

            Database database = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(new JdbcConnection(conn));

            Liquibase liquibase = new Liquibase(changeLogRelative, compositeAccessor, database);
            liquibase.update(new Contexts(), new LabelExpression());
        }

        LOGGER.info("ChangeLog aplicado: {}", toApplyPath);
    }
}
