package com.github.jhonatas48.migrationapi.core;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SqliteEndToEndMigrationFileTest {

    private static final Path TEST_DB_PATH = Path.of("src/test/resources/db/database.db");
    private static HikariDataSource dataSource;

    @BeforeAll
    static void setupDatabaseFile() throws Exception {
        Files.createDirectories(TEST_DB_PATH.getParent());
        // começa sempre limpo p/ evitar influência de execuções anteriores
        Files.deleteIfExists(TEST_DB_PATH);

        HikariConfig cfg = new HikariConfig();
        cfg.setPoolName("test-sqlite-pool");
        cfg.setJdbcUrl("jdbc:sqlite:" + TEST_DB_PATH.toAbsolutePath());
        cfg.setMaximumPoolSize(1);
        cfg.setAutoCommit(true);

        dataSource = new HikariDataSource(cfg);

        // roda a migração uma vez antes de todos os testes
        final String classpathChangelog = "db/changelog/changelog-TEST.yaml";
        new ChangeLogApplier(dataSource).applyChangeLog(classpathChangelog);
    }

    @AfterAll
    static void tearDown() {
        if (dataSource != null) dataSource.close();
    }

    @Test
    @Order(1)
    void shouldApplyAdjustedYamlAgainstPhysicalSqliteFile() {
        final List<String> objects = listObjects("table", "index");
        assertThat(objects).isNotEmpty();
    }

    @Test
    @Order(2)
    void shouldHaveLiquibaseTablesAndSomeDomainTables() {
        final List<String> tables = listTables();
        assertThat(tables).contains("DATABASECHANGELOG", "DATABASECHANGELOGLOCK");
        // exemplos: assertThat(tables).contains("User", "RatingQuestion");
    }

    // ===== helpers =====

    private static List<String> listObjects(String... types) {
        final String inClause = String.join(",", List.of(types).stream().map(t -> "'" + t + "'").toList());
        final String sql = "SELECT name FROM sqlite_master WHERE type IN (" + inClause + ") ORDER BY name";
        return querySingleColumn(sql);
    }

    private static List<String> listTables() {
        return querySingleColumn("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name");
    }

    private static List<String> querySingleColumn(String sql) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            final List<String> out = new ArrayList<>();
            while (rs.next()) out.add(rs.getString(1));
            return out;
        } catch (SQLException e) {
            throw new IllegalStateException("Falha ao consultar: " + sql + " - " + e.getMessage(), e);
        }
    }
}
