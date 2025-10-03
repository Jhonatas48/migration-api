package com.github.jhonatas48.migrationapi.core.sqlite;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Reconstrói tabelas no SQLite para embutir ALTERs de FK:
 * - Lê schema atual (PRAGMA table_info, foreign_key_list)
 * - Aplica remoções/adições de FKs
 * - Cria tabela temporária com o novo CREATE TABLE (inclui FKs)
 * - Copia dados, dropa original e renomeia
 * - Recria índices/uniques
 *
 * Observações:
 * - Mantém colunas, NOT NULL, DEFAULT e PK conforme PRAGMA table_info
 * - Mantém FKs existentes, removendo as que vão cair e adicionando as novas
 * - Recria índices (inclusive UNIQUE) via PRAGMA index_list/index_info
 */
public class SqliteTableRebuilder {

    public static final class ForeignKeySpec {
        private final String baseColumnsCsv;            // ex: "language_id"
        private final String referencedTable;           // ex: "Language"
        private final String referencedColumnsCsv;      // ex: "id"
        private final String onDelete;                  // ex: "CASCADE" | "" -> ignora
        private final String onUpdate;                  // ex: "NO ACTION" | "" -> ignora

        public ForeignKeySpec(String baseColumnsCsv,
                              String referencedTable,
                              String referencedColumnsCsv,
                              String onDelete,
                              String onUpdate) {
            this.baseColumnsCsv = baseColumnsCsv;
            this.referencedTable = referencedTable;
            this.referencedColumnsCsv = referencedColumnsCsv;
            this.onDelete = onDelete;
            this.onUpdate = onUpdate;
        }

        public String getBaseColumnsCsv() { return baseColumnsCsv; }
        public String getReferencedTable() { return referencedTable; }
        public String getReferencedColumnsCsv() { return referencedColumnsCsv; }
        public String getOnDelete() { return onDelete; }
        public String getOnUpdate() { return onUpdate; }

        @Override
        public String toString() {
            return baseColumnsCsv + " -> " + referencedTable + "(" + referencedColumnsCsv + ")"
                    + (onDelete == null || onDelete.isBlank() ? "" : " ON DELETE " + onDelete)
                    + (onUpdate == null || onUpdate.isBlank() ? "" : " ON UPDATE " + onUpdate);
        }
    }

    private final DataSource dataSource;

    public SqliteTableRebuilder(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void rebuildTableApplyingForeignKeyChanges(String tableName,
                                                      List<ForeignKeySpec> foreignKeysToAdd,
                                                      List<ForeignKeySpec> foreignKeysToDrop) throws SQLException {

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            try {
                execute(connection, "PRAGMA foreign_keys=OFF");

                TableInfo tableInfo = readCurrentTableInfo(connection, tableName);
                if (tableInfo.columns.isEmpty()) {
                    throw new IllegalStateException("Tabela não encontrada ou sem colunas: " + tableName);
                }

                List<ForeignKeySpec> initialFks = readCurrentForeignKeys(connection, tableName);

                // Remove as FKs que devem cair
                List<ForeignKeySpec> remainingFks = new ArrayList<>(initialFks);
                for (ForeignKeySpec toDrop : foreignKeysToDrop) {
                    remainingFks.removeIf(existing ->
                            sameFkByColumns(existing, toDrop) ||
                                    sameFkByTarget(existing, toDrop)
                    );
                }

                // Adiciona novas FKs
                remainingFks.addAll(foreignKeysToAdd);

                // Monta CREATE TABLE com colunas + PK + FKs (remainingFks)
                String createSql = buildCreateTableWithFks(tableName, tableInfo, remainingFks, "__tmp_" + tableName);

                // Índices existentes (para recriar depois)
                List<IndexInfo> indexes = readIndexes(connection, tableName);

                // Cria tabela temporária, copia, dropa e renomeia
                execute(connection, createSql);

                String columnList = tableInfo.columns.stream()
                        .map(c -> quote(c.name))
                        .collect(Collectors.joining(", "));
                execute(connection, "INSERT INTO " + quote("__tmp_" + tableName) +
                        "(" + columnList + ") SELECT " + columnList + " FROM " + quote(tableName));
                execute(connection, "DROP TABLE " + quote(tableName));
                execute(connection, "ALTER TABLE " + quote("__tmp_" + tableName) + " RENAME TO " + quote(tableName));

                // Recria índices
                for (IndexInfo idx : indexes) {
                    if (idx.isOriginPk) continue; // PK já foi incluída no CREATE TABLE
                    String idxName = idx.name;
                    String cols = idx.columns.stream().map(SqliteTableRebuilder::quote).collect(Collectors.joining(", "));
                    String unique = idx.unique ? "UNIQUE " : "";
                    execute(connection, "CREATE " + unique + "INDEX " + quote(idxName) + " ON " + quote(tableName) + " (" + cols + ")");
                }

                execute(connection, "PRAGMA foreign_keys=ON");

                connection.commit();
            } catch (Throwable t) {
                connection.rollback();
                throw t;
            } finally {
                connection.setAutoCommit(true);
            }
        }
    }

    private boolean sameFkByColumns(ForeignKeySpec a, ForeignKeySpec b) {
        return normalizeCsv(a.baseColumnsCsv).equalsIgnoreCase(normalizeCsv(b.baseColumnsCsv));
    }
    private boolean sameFkByTarget(ForeignKeySpec a, ForeignKeySpec b) {
        return a.referencedTable.equalsIgnoreCase(b.referencedTable)
                && normalizeCsv(a.referencedColumnsCsv).equalsIgnoreCase(normalizeCsv(b.referencedColumnsCsv));
    }
    private static String normalizeCsv(String csv) {
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .collect(Collectors.joining(","));
    }

    private static class TableInfo {
        static class Column {
            final String name;
            final String type;
            final boolean notNull;
            final String defaultValue; // pode vir como expressão
            final boolean primaryKey;

            Column(String name, String type, boolean notNull, String defaultValue, boolean primaryKey) {
                this.name = name;
                this.type = type;
                this.notNull = notNull;
                this.defaultValue = defaultValue;
                this.primaryKey = primaryKey;
            }
        }
        final List<Column> columns = new ArrayList<>();
    }

    private static class IndexInfo {
        final String name;
        final boolean unique;
        final boolean isOriginPk;
        final List<String> columns = new ArrayList<>();

        IndexInfo(String name, boolean unique, boolean isOriginPk) {
            this.name = name;
            this.unique = unique;
            this.isOriginPk = isOriginPk;
        }
    }

    private TableInfo readCurrentTableInfo(Connection c, String table) throws SQLException {
        TableInfo info = new TableInfo();
        try (PreparedStatement ps = c.prepareStatement("PRAGMA table_info(" + quote(table) + ")")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("name");
                    String type = rs.getString("type");
                    boolean notnull = rs.getInt("notnull") == 1;
                    String dflt = rs.getString("dflt_value");
                    boolean pk = rs.getInt("pk") == 1;
                    info.columns.add(new TableInfo.Column(name, type, notnull, dflt, pk));
                }
            }
        }
        return info;
    }

    private List<ForeignKeySpec> readCurrentForeignKeys(Connection c, String table) throws SQLException {
        List<ForeignKeySpec> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement("PRAGMA foreign_key_list(" + quote(table) + ")")) {
            try (ResultSet rs = ps.executeQuery()) {
                Map<Integer, List<Row>> grouped = new LinkedHashMap<>();
                while (rs.next()) {
                    int id = rs.getInt("id");
                    String tableRef = rs.getString("table");
                    String fromCol = rs.getString("from");
                    String toCol = rs.getString("to");
                    String onUpdate = rs.getString("on_update");
                    String onDelete = rs.getString("on_delete");
                    grouped.computeIfAbsent(id, k -> new ArrayList<>()).add(new Row(tableRef, fromCol, toCol, onUpdate, onDelete));
                }
                for (List<Row> rows : grouped.values()) {
                    String refTable = rows.get(0).tableRef;
                    String onUpdate = rows.get(0).onUpdate;
                    String onDelete = rows.get(0).onDelete;

                    String baseCols = rows.stream().map(r -> r.fromCol).collect(Collectors.joining(","));
                    String refCols  = rows.stream().map(r -> r.toCol).collect(Collectors.joining(","));
                    out.add(new ForeignKeySpec(baseCols, refTable, refCols, onDelete, onUpdate));
                }
            }
        }
        return out;
    }

    private static class Row {
        final String tableRef;
        final String fromCol;
        final String toCol;
        final String onUpdate;
        final String onDelete;
        Row(String tableRef, String fromCol, String toCol, String onUpdate, String onDelete) {
            this.tableRef = tableRef; this.fromCol = fromCol; this.toCol = toCol; this.onUpdate = onUpdate; this.onDelete = onDelete;
        }
    }

    private List<IndexInfo> readIndexes(Connection c, String table) throws SQLException {
        List<IndexInfo> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement("PRAGMA index_list(" + quote(table) + ")")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("name");
                    boolean unique = rs.getInt("unique") == 1;
                    String origin = rs.getString("origin"); // 'pk' para primary key implícita
                    IndexInfo idx = new IndexInfo(name, unique, "pk".equalsIgnoreCase(origin));
                    out.add(idx);

                    if (!idx.isOriginPk) {
                        try (PreparedStatement ps2 = c.prepareStatement("PRAGMA index_info(" + quote(name) + ")")) {
                            try (ResultSet rs2 = ps2.executeQuery()) {
                                while (rs2.next()) {
                                    idx.columns.add(rs2.getString("name"));
                                }
                            }
                        }
                    }
                }
            }
        }
        return out;
    }

    private String buildCreateTableWithFks(String originalTable,
                                           TableInfo tableInfo,
                                           List<ForeignKeySpec> fks,
                                           String tempName) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(quote(tempName)).append(" (\n");

        // Colunas (com PK inline quando a PK é single-column; para PK composta, aplicamos no final)
        List<String> pkCols = tableInfo.columns.stream()
                .filter(c -> c.primaryKey)
                .map(c -> c.name)
                .collect(Collectors.toList());

        List<String> columnDefs = new ArrayList<>();
        for (SqliteTableRebuilder.TableInfo.Column c : tableInfo.columns) {
            StringBuilder col = new StringBuilder();
            col.append("  ").append(quote(c.name)).append(" ").append(c.type == null ? "" : c.type);

            // PK de uma coluna pode vir inline (SQLite aceita)
            if (pkCols.size() == 1 && c.primaryKey) {
                col.append(" PRIMARY KEY");
            }
            if (c.notNull) col.append(" NOT NULL");
            if (c.defaultValue != null) col.append(" DEFAULT ").append(c.defaultValue);
            columnDefs.add(col.toString());
        }

        // PK composta
        if (pkCols.size() > 1) {
            String pk = pkCols.stream().map(SqliteTableRebuilder::quote).collect(Collectors.joining(", "));
            columnDefs.add("  PRIMARY KEY (" + pk + ")");
        }

        // FKs
        for (ForeignKeySpec fk : fks) {
            if (fk.getReferencedTable() == null || fk.getReferencedTable().isBlank()) continue;
            String baseCols = Arrays.stream(fk.getBaseColumnsCsv().split(",")).map(String::trim)
                    .map(SqliteTableRebuilder::quote).collect(Collectors.joining(", "));
            String refCols  = Arrays.stream(fk.getReferencedColumnsCsv().split(",")).map(String::trim)
                    .map(SqliteTableRebuilder::quote).collect(Collectors.joining(", "));

            StringBuilder fkDef = new StringBuilder();
            fkDef.append("  FOREIGN KEY (").append(baseCols).append(")")
                    .append(" REFERENCES ").append(quote(fk.getReferencedTable()))
                    .append(" (").append(refCols).append(")");

            if (fk.getOnDelete() != null && !fk.getOnDelete().isBlank()) {
                fkDef.append(" ON DELETE ").append(fk.getOnDelete());
            }
            if (fk.getOnUpdate() != null && !fk.getOnUpdate().isBlank()) {
                fkDef.append(" ON UPDATE ").append(fk.getOnUpdate());
            }

            columnDefs.add(fkDef.toString());
        }

        sb.append(String.join(",\n", columnDefs)).append("\n)");
        return sb.toString();
    }

    private static void execute(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static String quote(String id) {
        // Aspas duplas preservam case e evitam conflitos com keywords
        return "\"" + id.replace("\"", "\"\"") + "\"";
    }
}
