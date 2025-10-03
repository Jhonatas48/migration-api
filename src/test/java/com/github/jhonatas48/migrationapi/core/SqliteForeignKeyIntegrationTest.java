package com.github.jhonatas48.migrationapi.core;

import com.github.jhonatas48.migrationapi.core.sqlite.SqliteForeignKeyChangeSetRewriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integração: SQLite em memória
 *
 * Valida que um changeSet com addForeignKeyConstraint é reescrito para
 * "recriar tabela + copiar dados + rename" e que a FK aparece no PRAGMA.
 */
public class SqliteForeignKeyIntegrationTest {

    private Connection keepAliveConnection; // mantém a memória viva enquanto testamos

    private DataSource createInMemorySharedSqliteDataSource() throws Exception {
        // Importante: usar shared cache para que conexões do pool/ Liquibase
        // compartilhem o mesmo DB em memória.
        // Ex.: jdbc:sqlite:file:mem-it-db?mode=memory&cache=shared
        final String jdbcUrl = "jdbc:sqlite:file:mem-it-db?mode=memory&cache=shared";
        Class.forName("org.sqlite.JDBC");
        keepAliveConnection = DriverManager.getConnection(jdbcUrl);

        // Ativa FK enforcement na sessão "keep-alive"
        try (Statement st = keepAliveConnection.createStatement()) {
            st.execute("PRAGMA foreign_keys = ON");
        }

        // DataSource mínimo baseado no DriverManager (sem pool)
        return new DataSource() {
            @Override public Connection getConnection() throws SQLException { return DriverManager.getConnection(jdbcUrl); }
            @Override public Connection getConnection(String u, String p) throws SQLException { return getConnection(); }
            @Override public <T> T unwrap(Class<T> iface) { throw new UnsupportedOperationException(); }
            @Override public boolean isWrapperFor(Class<?> iface) { return false; }
            @Override public java.io.PrintWriter getLogWriter() { return null; }
            @Override public void setLogWriter(java.io.PrintWriter out) { }
            @Override public void setLoginTimeout(int seconds) { }
            @Override public int getLoginTimeout() { return 0; }
            @Override public java.util.logging.Logger getParentLogger() { return java.util.logging.Logger.getGlobal(); }
        };
    }

    @AfterEach
    void teardown() throws Exception {
        if (keepAliveConnection != null && !keepAliveConnection.isClosed()) {
            keepAliveConnection.close();
        }
    }

    @Test
    void addForeignKeyChangeSet_isRewrittenAndApplied_fkExistsInPragma() throws Exception {
        DataSource dataSource = createInMemorySharedSqliteDataSource();

        // 1) YAML “ingênuo” com addForeignKeyConstraint (Liquibase padrão)
        String originalYaml = ""
                + "databaseChangeLog:\n"
                + "- changeSet:\n"
                + "    id: 1\n"
                + "    author: it\n"
                + "    changes:\n"
                + "    - createTable:\n"
                + "        tableName: parent\n"
                + "        columns:\n"
                + "        - column:\n"
                + "            name: id\n"
                + "            type: INTEGER\n"
                + "            constraints:\n"
                + "              primaryKey: true\n"
                + "              nullable: false\n"
                + "    - createTable:\n"
                + "        tableName: child\n"
                + "        columns:\n"
                + "        - column:\n"
                + "            name: id\n"
                + "            type: INTEGER\n"
                + "            constraints:\n"
                + "              primaryKey: true\n"
                + "              nullable: false\n"
                + "        - column:\n"
                + "            name: parent_id\n"
                + "            type: INTEGER\n"
                + "    - addForeignKeyConstraint:\n"
                + "        baseTableName: child\n"
                + "        baseColumnNames: parent_id\n"
                + "        referencedTableName: parent\n"
                + "        referencedColumnNames: id\n"
                + "        constraintName: fk_child_parent\n";

        // 2) Reescreve para “SQLite-friendly” (sem addForeignKeyConstraint)
        SqliteForeignKeyChangeSetRewriter rewriter = new SqliteForeignKeyChangeSetRewriter();
        String sanitizedYaml = rewriter.rewrite(originalYaml);

        assertNotNull(sanitizedYaml, "YAML reescrito não pode ser nulo");
        assertFalse(sanitizedYaml.contains("addForeignKeyConstraint"),
                "YAML final deve remover addForeignKeyConstraint para SQLite");
        assertTrue(sanitizedYaml.contains("createTable"),
                "YAML final deve conter createTable para recriação de tabela");
        assertTrue(
                sanitizedYaml.contains("renameTable") ||
                        sanitizedYaml.contains("renameTable:") ||
                        sanitizedYaml.matches("(?s).*ALTER\\s+TABLE\\s+\\S+\\s+RENAME\\s+TO\\s+\\S+__old.*"),
                "YAML final deve conter renameTable OU ALTER TABLE ... RENAME TO para finalizar a troca"
        );

        // 3) Aplica via ChangeLogApplier
        Path tempFile = Files.createTempFile("liquibase-it-", ".yaml");
        Files.writeString(tempFile, sanitizedYaml, StandardCharsets.UTF_8);

        ChangeLogApplier applier = new ChangeLogApplier(dataSource);
        applier.applyChangeLog(tempFile.toAbsolutePath().toString());

        // 4) Confere que a FK existe no SQLite
        try (Connection c = dataSource.getConnection()) {
            // garante FK enforcement nesta connection também
            try (Statement st = c.createStatement()) {
                st.execute("PRAGMA foreign_keys = ON");
            }

            List<PragmaFkRow> fkRows = readForeignKeys(c, "child");
            assertEquals(1, fkRows.size(), "Deve haver exatamente 1 FK em child");
            PragmaFkRow fk = fkRows.get(0);
            assertEquals("parent", fk.referencedTable, "FK deve referenciar a tabela parent");
            assertEquals("parent_id", fk.fromColumn, "Coluna local deve ser parent_id");
            assertEquals("id", fk.toColumn, "Coluna referenciada deve ser id");
        }
    }

    // Lê o resultado do PRAGMA foreign_key_list('<tabela>')
    private static List<PragmaFkRow> readForeignKeys(Connection c, String table) throws SQLException {
        String sql = "PRAGMA foreign_key_list('" + table + "')";
        try (PreparedStatement ps = c.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            List<PragmaFkRow> list = new ArrayList<>();
            while (rs.next()) {
                // colunas do PRAGMA: id, seq, table, from, to, on_update, on_delete, match
                String referencedTable = rs.getString("table");
                String fromColumn = rs.getString("from");
                String toColumn = rs.getString("to");
                list.add(new PragmaFkRow(referencedTable, fromColumn, toColumn));
            }
            return list;
        }
    }

    private static final class PragmaFkRow {
        final String referencedTable;
        final String fromColumn;
        final String toColumn;
        PragmaFkRow(String referencedTable, String fromColumn, String toColumn) {
            this.referencedTable = referencedTable;
            this.fromColumn = fromColumn;
            this.toColumn = toColumn;
        }
    }
}
