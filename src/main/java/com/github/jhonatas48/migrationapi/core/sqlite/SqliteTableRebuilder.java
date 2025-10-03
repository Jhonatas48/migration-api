package com.github.jhonatas48.migrationapi.core.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Reconstrói tabelas no SQLite para aplicar alterações de chaves estrangeiras (FK) e preservar
 * corretamente constraints/atributos de coluna (NOT NULL, DEFAULT, PK e AUTOINCREMENT).
 *
 * Fluxo robusto:
 *  1) PRAGMA foreign_keys = OFF (com restauração garantida)
 *  2) Lê schema atual: PRAGMAs table_info / foreign_key_list / index_list / index_info
 *  3) Lê DDL original (sqlite_master.sql) para detectar AUTOINCREMENT, quando existir
 *  4) Calcula FKs finais (remove solicitadas, adiciona novas)
 *  5) Cria tabela temporária com schema final (inclui FKs)
 *  6) Copia dados coluna-a-coluna
 *  7) Swap atômico via rename: original -> __bak_, temp -> original
 *  8) Recria índices
 *  9) DROP __bak_ (com proteção de PRAGMA), PRAGMA foreign_keys = ON e foreign_key_check
 *
 *  - Restauração de estado (auto-commit e PRAGMA foreign_keys)
 *  - Mensagens de erro detalhadas com diagnóstico de FKs
 */
public class SqliteTableRebuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqliteTableRebuilder.class);

    // PRAGMAs e SQLs utilitários
    private static final String PRAGMA_FOREIGN_KEYS_ON      = "PRAGMA foreign_keys=ON";
    private static final String PRAGMA_FOREIGN_KEYS_OFF     = "PRAGMA foreign_keys=OFF";
    private static final String PRAGMA_FOREIGN_KEYS_STATE   = "PRAGMA foreign_keys";
    private static final String PRAGMA_TABLE_INFO           = "PRAGMA table_info(%s)";
    private static final String PRAGMA_INDEX_LIST           = "PRAGMA index_list(%s)";
    private static final String PRAGMA_INDEX_INFO           = "PRAGMA index_info(%s)";
    private static final String PRAGMA_FK_LIST              = "PRAGMA foreign_key_list(%s)";
    private static final String PRAGMA_FK_CHECK             = "PRAGMA foreign_key_check";
    private static final String PRAGMA_LEGACY_ALTER_ON      = "PRAGMA legacy_alter_table=ON";

    private static final String TEMP_TABLE_PREFIX           = "__tmp_";
    private static final String BACKUP_TABLE_PREFIX         = "__bak_";

    /**
     * Especificação de uma FK (colunas base e referenciadas + ações).
     */
    public static final class ForeignKeySpec {
        private final String baseColumnsCsv;
        private final String referencedTable;
        private final String referencedColumnsCsv;
        private final String onDeleteAction;
        private final String onUpdateAction;

        public ForeignKeySpec(String baseColumnsCsv,
                              String referencedTable,
                              String referencedColumnsCsv,
                              String onDelete,
                              String onUpdate) {
            this.baseColumnsCsv = baseColumnsCsv;
            this.referencedTable = referencedTable;
            this.referencedColumnsCsv = referencedColumnsCsv;
            this.onDeleteAction = onDelete;
            this.onUpdateAction = onUpdate;
        }

        public String getBaseColumnsCsv()       { return baseColumnsCsv; }
        public String getReferencedTable()      { return referencedTable; }
        public String getReferencedColumnsCsv() { return referencedColumnsCsv; }
        public String getOnDelete()             { return onDeleteAction; }
        public String getOnUpdate()             { return onUpdateAction; }

        @Override
        public String toString() {
            StringBuilder description = new StringBuilder();
            description.append(baseColumnsCsv)
                    .append(" -> ")
                    .append(referencedTable)
                    .append("(").append(referencedColumnsCsv).append(")");
            if (onDeleteAction != null && !onDeleteAction.isBlank()) description.append(" ON DELETE ").append(onDeleteAction);
            if (onUpdateAction != null && !onUpdateAction.isBlank()) description.append(" ON UPDATE ").append(onUpdateAction);
            return description.toString();
        }
    }

    private final DataSource dataSource;

    public SqliteTableRebuilder(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Reconstrói a tabela para aplicar adições e remoções de FKs preservando NOT NULL, DEFAULT,
     * PK e AUTOINCREMENT quando aplicável.
     *
     * @param tableName         Nome da tabela a reconstruir
     * @param foreignKeysToAdd  FKs a adicionar
     * @param foreignKeysToDrop FKs a remover
     */
    public void rebuildTableApplyingForeignKeyChanges(String tableName,
                                                      List<ForeignKeySpec> foreignKeysToAdd,
                                                      List<ForeignKeySpec> foreignKeysToDrop) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            try {
                // 1) FK OFF + facilitadores de ALTER em versões antigas
                disableForeignKeyEnforcement(connection);
                safelyExecute(connection, PRAGMA_LEGACY_ALTER_ON);

                // 2) Metadados atuais
                TableSchema currentSchema = readCurrentTableSchema(connection, tableName);
                if (currentSchema.columns.isEmpty()) {
                    throw new IllegalStateException("Tabela não encontrada ou sem colunas: " + tableName);
                }

                // Lê DDL original para detectar AUTOINCREMENT (não está em PRAGMA table_info)
                String originalCreateTableSql = readCreateTableSql(connection, tableName);

                // PKs atuais (para filtrar possíveis colunas AUTOINCREMENT)
                List<String> currentPrimaryKeyColumns = currentSchema.columns.stream()
                        .filter(c -> c.partOfPrimaryKey)
                        .map(c -> c.name)
                        .toList();

                // AUTOINCREMENT só é válido quando a PK é de uma única coluna INTEGER
                Set<String> autoIncrementColumns = currentPrimaryKeyColumns.size() == 1
                        ? detectAutoIncrementColumns(originalCreateTableSql, currentPrimaryKeyColumns)
                        : Collections.emptySet();

                // FKs finais (remove solicitadas, adiciona novas)
                List<ForeignKeySpec> currentForeignKeys = readCurrentForeignKeys(connection, tableName);
                List<ForeignKeySpec> desiredForeignKeys = computeFinalForeignKeys(
                        currentForeignKeys, foreignKeysToAdd, foreignKeysToDrop
                );

                // 3) Nomes e SQL de criação
                String tempTableName   = TEMP_TABLE_PREFIX + tableName;
                String backupTableName = BACKUP_TABLE_PREFIX + tableName;
                String createTempSql   = buildCreateTableStatement(
                        tableName, currentSchema, desiredForeignKeys, tempTableName, autoIncrementColumns
                );
                List<IndexDefinition> currentIndexes = readCurrentIndexes(connection, tableName);

                // 4) Cria temp e copia dados
                createTemporaryTable(connection, createTempSql);
                copyDataSameColumns(connection, tableName, tempTableName, currentSchema);

                // 5) Swap via rename com backup
                renameTableSafely(connection, tableName, backupTableName); // original -> backup
                renameTableSafely(connection, tempTableName, tableName);   // temp -> original

                // 6) Recria índices na tabela final
                recreateIndexes(connection, tableName, currentIndexes);

                // 7) Remove backup (com proteção de PRAGMA)
                dropTableSafely(connection, backupTableName);

                // 8) FK ON e validação
                enableForeignKeyEnforcement(connection);
                validateReferentialIntegrityOrFail(connection, tableName);

                connection.commit();
                LOGGER.info("Rebuild da tabela '{}' concluído com sucesso.", tableName);
            } catch (Throwable failure) {
                safeRollback(connection);
                // Garante que não deixe a conexão com FK desligado
                safelyExecute(connection, PRAGMA_FOREIGN_KEYS_ON);
                throw failure;
            } finally {
                // Restaura auto-commit e força FK ON para não vazar estado
                safelyExecute(connection, PRAGMA_FOREIGN_KEYS_ON);
                connection.setAutoCommit(previousAutoCommit);
            }
        }
    }

    // ===================== Orquestração de Passos =====================

    private void disableForeignKeyEnforcement(Connection connection) throws SQLException {
        executeStatement(connection, PRAGMA_FOREIGN_KEYS_OFF);
    }

    private void enableForeignKeyEnforcement(Connection connection) throws SQLException {
        executeStatement(connection, PRAGMA_FOREIGN_KEYS_ON);
    }

    private void createTemporaryTable(Connection connection, String createTempSql) throws SQLException {
        executeStatement(connection, createTempSql);
    }

    private void copyDataSameColumns(Connection connection,
                                     String sourceTable,
                                     String targetTempTable,
                                     TableSchema schema) throws SQLException {
        String columnList = schema.columns.stream()
                .map(column -> quoteIdentifier(column.name))
                .collect(Collectors.joining(", "));
        String insertSql = "INSERT INTO " + quoteIdentifier(targetTempTable)
                + "(" + columnList + ") SELECT " + columnList + " FROM " + quoteIdentifier(sourceTable);
        executeStatement(connection, insertSql);
    }

    private void recreateIndexes(Connection connection,
                                 String tableName,
                                 List<IndexDefinition> previousIndexes) throws SQLException {
        for (IndexDefinition index : previousIndexes) {
            if (index.createdFromPrimaryKey) continue; // PK já está no CREATE TABLE
            String indexColumns = index.columns.stream()
                    .map(SqliteTableRebuilder::quoteIdentifier)
                    .collect(Collectors.joining(", "));
            String uniqueClause = index.unique ? "UNIQUE " : "";
            String createIndexSql = "CREATE " + uniqueClause + "INDEX " + quoteIdentifier(index.name)
                    + " ON " + quoteIdentifier(tableName) + " (" + indexColumns + ")";
            executeStatement(connection, createIndexSql);
        }
    }

    private void validateReferentialIntegrityOrFail(Connection connection,
                                                    String rebuiltTableName) throws SQLException {
        List<ForeignKeyViolation> violations = runForeignKeyCheck(connection);
        if (violations.isEmpty()) return;

        String errorMessage = buildForeignKeyViolationMessage(rebuiltTableName, violations, connection);
        LOGGER.error(errorMessage);
        throw new IllegalStateException(errorMessage);
    }

    // ===================== Helpers de Swap/Drop Seguros =====================

    private void renameTableSafely(Connection connection, String fromTable, String toTable) throws SQLException {
        boolean wasOn = isForeignKeysOn(connection);
        if (wasOn) setForeignKeys(connection, false);
        try {
            executeStatement(connection,
                    "ALTER TABLE " + quoteIdentifier(fromTable) + " RENAME TO " + quoteIdentifier(toTable));
        } finally {
            if (wasOn) setForeignKeys(connection, true);
        }
    }

    private void dropTableSafely(Connection connection, String tableName) throws SQLException {
        boolean wasOn = isForeignKeysOn(connection);
        if (wasOn) setForeignKeys(connection, false);
        try {
            executeStatement(connection, "DROP TABLE " + quoteIdentifier(tableName));
        } finally {
            if (wasOn) setForeignKeys(connection, true);
        }
    }

    private static boolean isForeignKeysOn(Connection connection) {
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(PRAGMA_FOREIGN_KEYS_STATE)) {
            if (rs.next()) return rs.getInt(1) == 1;
        } catch (SQLException ignored) { }
        return false;
    }

    private static void setForeignKeys(Connection connection, boolean turnOn) {
        safelyExecute(connection, turnOn ? PRAGMA_FOREIGN_KEYS_ON : PRAGMA_FOREIGN_KEYS_OFF);
    }

    // ===================== Cálculo de FKs Finais =====================

    private List<ForeignKeySpec> computeFinalForeignKeys(List<ForeignKeySpec> currentForeignKeys,
                                                         List<ForeignKeySpec> foreignKeysToAdd,
                                                         List<ForeignKeySpec> foreignKeysToDrop) {
        List<ForeignKeySpec> result = new ArrayList<>(currentForeignKeys);
        for (ForeignKeySpec toDrop : foreignKeysToDrop) {
            result.removeIf(existing ->
                    sameByBaseColumns(existing, toDrop) || sameByReferencedTarget(existing, toDrop)
            );
        }
        result.addAll(foreignKeysToAdd);
        return result;
    }

    private boolean sameByBaseColumns(ForeignKeySpec a, ForeignKeySpec b) {
        return normalizeCsv(a.baseColumnsCsv).equalsIgnoreCase(normalizeCsv(b.baseColumnsCsv));
    }

    private boolean sameByReferencedTarget(ForeignKeySpec a, ForeignKeySpec b) {
        return a.referencedTable.equalsIgnoreCase(b.referencedTable)
                && normalizeCsv(a.referencedColumnsCsv).equalsIgnoreCase(normalizeCsv(b.referencedColumnsCsv));
    }

    private static String normalizeCsv(String csv) {
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .collect(Collectors.joining(","));
    }

    // ===================== Modelos Internos =====================

    private static final class TableSchema {
        private final List<TableColumn> columns = new ArrayList<>();
    }

    private static final class TableColumn {
        private final String name;
        private final String typeDeclaration;
        private final boolean notNull;
        private final String defaultExpression;
        private final boolean partOfPrimaryKey;

        private TableColumn(String name,
                            String typeDeclaration,
                            boolean notNull,
                            String defaultExpression,
                            boolean partOfPrimaryKey) {
            this.name = name;
            this.typeDeclaration = typeDeclaration;
            this.notNull = notNull;
            this.defaultExpression = defaultExpression;
            this.partOfPrimaryKey = partOfPrimaryKey;
        }
    }

    private static final class IndexDefinition {
        private final String name;
        private final boolean unique;
        private final boolean createdFromPrimaryKey; // origin == 'pk'
        private final List<String> columns = new ArrayList<>();

        private IndexDefinition(String name, boolean unique, boolean createdFromPrimaryKey) {
            this.name = name;
            this.unique = unique;
            this.createdFromPrimaryKey = createdFromPrimaryKey;
        }
    }

    private static final class ForeignKeyRow {
        private final String referencedTable;
        private final String baseColumn;
        private final String referencedColumn;
        private final String onUpdateAction;
        private final String onDeleteAction;

        private ForeignKeyRow(String referencedTable,
                              String baseColumn,
                              String referencedColumn,
                              String onUpdateAction,
                              String onDeleteAction) {
            this.referencedTable = referencedTable;
            this.baseColumn = baseColumn;
            this.referencedColumn = referencedColumn;
            this.onUpdateAction = onUpdateAction;
            this.onDeleteAction = onDeleteAction;
        }
    }

    private static final class ForeignKeyViolation {
        private final String violatingTable;
        private final long violatingRowId;
        private final String referencedParentTable;
        private final int foreignKeyId;

        private ForeignKeyViolation(String violatingTable,
                                    long violatingRowId,
                                    String referencedParentTable,
                                    int foreignKeyId) {
            this.violatingTable = violatingTable;
            this.violatingRowId = violatingRowId;
            this.referencedParentTable = referencedParentTable;
            this.foreignKeyId = foreignKeyId;
        }
    }

    // ===================== Leitura de Metadados =====================

    private TableSchema readCurrentTableSchema(Connection connection, String tableName) throws SQLException {
        TableSchema schema = new TableSchema();
        String sql = String.format(PRAGMA_TABLE_INFO, quoteIdentifier(tableName));

        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                String columnName      = resultSet.getString("name");
                String typeDeclaration = resultSet.getString("type");
                boolean notNull        = resultSet.getInt("notnull") == 1;
                String defaultExpr     = resultSet.getString("dflt_value");
                boolean isPrimaryKey   = resultSet.getInt("pk") == 1;

                schema.columns.add(new TableColumn(columnName, typeDeclaration, notNull, defaultExpr, isPrimaryKey));
            }
        }
        return schema;
    }

    private List<ForeignKeySpec> readCurrentForeignKeys(Connection connection, String tableName) throws SQLException {
        Map<Integer, List<ForeignKeyRow>> groupedByConstraint = new LinkedHashMap<>();
        String sql = String.format(PRAGMA_FK_LIST, quoteIdentifier(tableName));

        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet result = statement.executeQuery()) {

            while (result.next()) {
                int constraintId      = result.getInt("id");
                String referencedTable = result.getString("table");
                String baseColumn      = result.getString("from");
                String referencedColumn= result.getString("to");
                String onUpdate        = result.getString("on_update");
                String onDelete        = result.getString("on_delete");

                groupedByConstraint
                        .computeIfAbsent(constraintId, k -> new ArrayList<>())
                        .add(new ForeignKeyRow(referencedTable, baseColumn, referencedColumn, onUpdate, onDelete));
            }
        }

        List<ForeignKeySpec> foreignKeys = new ArrayList<>();
        for (List<ForeignKeyRow> rowsOfConstraint : groupedByConstraint.values()) {
            String referencedTable       = rowsOfConstraint.get(0).referencedTable;
            String onUpdate              = rowsOfConstraint.get(0).onUpdateAction;
            String onDelete              = rowsOfConstraint.get(0).onDeleteAction;
            String baseColumnsCsv        = rowsOfConstraint.stream().map(r -> r.baseColumn).collect(Collectors.joining(","));
            String referencedColumnsCsv  = rowsOfConstraint.stream().map(r -> r.referencedColumn).collect(Collectors.joining(","));

            foreignKeys.add(new ForeignKeySpec(baseColumnsCsv, referencedTable, referencedColumnsCsv, onDelete, onUpdate));
        }

        return foreignKeys;
    }

    private List<IndexDefinition> readCurrentIndexes(Connection connection, String tableName) throws SQLException {
        List<IndexDefinition> indexDefinitions = new ArrayList<>();
        String sql = String.format(PRAGMA_INDEX_LIST, quoteIdentifier(tableName));

        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet result = statement.executeQuery()) {

            while (result.next()) {
                String indexName = result.getString("name");
                boolean unique   = result.getInt("unique") == 1;
                String origin    = result.getString("origin"); // 'pk' quando deriva da PK
                IndexDefinition indexDefinition = new IndexDefinition(indexName, unique, "pk".equalsIgnoreCase(origin));
                indexDefinitions.add(indexDefinition);

                if (!indexDefinition.createdFromPrimaryKey) {
                    String indexInfoSql = String.format(PRAGMA_INDEX_INFO, quoteIdentifier(indexName));
                    try (PreparedStatement columnsStmt = connection.prepareStatement(indexInfoSql);
                         ResultSet columnsRs = columnsStmt.executeQuery()) {
                        while (columnsRs.next()) {
                            indexDefinition.columns.add(columnsRs.getString("name"));
                        }
                    }
                }
            }
        }

        return indexDefinitions;
    }

    // ===================== Construção do CREATE TABLE =====================

    private String buildCreateTableStatement(String originalTableName,
                                             TableSchema schema,
                                             List<ForeignKeySpec> foreignKeys,
                                             String tempTableName,
                                             Set<String> autoIncrementColumns) {
        StringBuilder create = new StringBuilder();
        create.append("CREATE TABLE ").append(quoteIdentifier(tempTableName)).append(" (\n");

        List<String> primaryKeyColumns = schema.columns.stream()
                .filter(column -> column.partOfPrimaryKey)
                .map(column -> column.name)
                .collect(Collectors.toList());

        List<String> columnDefinitions = new ArrayList<>();

        for (TableColumn column : schema.columns) {
            columnDefinitions.add(buildColumnDefinition(primaryKeyColumns, column, autoIncrementColumns));
        }

        // PK composta
        if (primaryKeyColumns.size() > 1) {
            String pkList = primaryKeyColumns.stream()
                    .map(SqliteTableRebuilder::quoteIdentifier)
                    .collect(Collectors.joining(", "));
            columnDefinitions.add("  PRIMARY KEY (" + pkList + ")");
        }

        // FKs
        for (ForeignKeySpec fk : foreignKeys) {
            if (fk.getReferencedTable() == null || fk.getReferencedTable().isBlank()) continue;
            columnDefinitions.add(buildForeignKeyDefinition(fk));
        }

        create.append(String.join(",\n", columnDefinitions)).append("\n)");
        return create.toString();
    }

    private String buildColumnDefinition(List<String> primaryKeyColumns,
                                         TableColumn column,
                                         Set<String> autoIncrementColumns) {
        StringBuilder definition = new StringBuilder();
        definition.append("  ").append(quoteIdentifier(column.name)).append(" ")
                .append(column.typeDeclaration == null ? "" : column.typeDeclaration);

        boolean isSingleColumnPk = primaryKeyColumns.size() == 1 && column.partOfPrimaryKey;
        if (isSingleColumnPk) {
            definition.append(" PRIMARY KEY");

            // AUTOINCREMENT só é válido para "INTEGER PRIMARY KEY" (uma única coluna).
            boolean isIntegerType = column.typeDeclaration != null
                    && column.typeDeclaration.trim().equalsIgnoreCase("INTEGER");
            if (isIntegerType && autoIncrementColumns.contains(column.name)) {
                definition.append(" AUTOINCREMENT");
            }
        }

        if (column.notNull) {
            definition.append(" NOT NULL");
        }
        if (column.defaultExpression != null) {
            // dflt_value já vem como literal/expressão do SQLite (ex.: CURRENT_TIMESTAMP, 'abc', 0, (datetime('now')))
            definition.append(" DEFAULT ").append(column.defaultExpression);
        }
        return definition.toString();
    }

    private String buildForeignKeyDefinition(ForeignKeySpec foreignKey) {
        String baseColumnList = Arrays.stream(foreignKey.getBaseColumnsCsv().split(","))
                .map(String::trim)
                .map(SqliteTableRebuilder::quoteIdentifier)
                .collect(Collectors.joining(", "));

        String referencedColumnList = Arrays.stream(foreignKey.getReferencedColumnsCsv().split(","))
                .map(String::trim)
                .map(SqliteTableRebuilder::quoteIdentifier)
                .collect(Collectors.joining(", "));

        StringBuilder definition = new StringBuilder();
        definition.append("  FOREIGN KEY (").append(baseColumnList).append(")")
                .append(" REFERENCES ").append(quoteIdentifier(foreignKey.getReferencedTable()))
                .append(" (").append(referencedColumnList).append(")");

        if (foreignKey.getOnDelete() != null && !foreignKey.getOnDelete().isBlank()) {
            definition.append(" ON DELETE ").append(foreignKey.getOnDelete());
        }
        if (foreignKey.getOnUpdate() != null && !foreignKey.getOnUpdate().isBlank()) {
            definition.append(" ON UPDATE ").append(foreignKey.getOnUpdate());
        }
        return definition.toString();
    }

    // ===================== Diagnóstico de Integridade =====================

    private List<ForeignKeyViolation> runForeignKeyCheck(Connection connection) {
        List<ForeignKeyViolation> violations = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(PRAGMA_FK_CHECK)) {
            while (result.next()) {
                String table  = safeGetString(result, "table");
                long rowId    = safeGetLong(result, "rowid");
                String parent = safeGetString(result, "parent");
                int fkid      = safeGetInt(result, "fkid");
                violations.add(new ForeignKeyViolation(table, rowId, parent, fkid));
            }
        } catch (SQLException e) {
            LOGGER.warn("Falha ao executar PRAGMA foreign_key_check: {}", e.getMessage());
        }
        return violations;
    }

    private String buildForeignKeyViolationMessage(String rebuiltTableName,
                                                   List<ForeignKeyViolation> violations,
                                                   Connection connection) {
        String header = "Violação(ões) de integridade referencial após rebuild da tabela '" + rebuiltTableName + "'.";
        String details = violations.stream()
                .map(v -> "table=" + v.violatingTable
                        + ", rowid=" + v.violatingRowId
                        + ", parent=" + v.referencedParentTable
                        + ", fkid=" + v.foreignKeyId)
                .collect(Collectors.joining("\n"));

        String definitions = buildForeignKeyDefinitionsForTables(connection, violations);
        return header + "\n" + details + definitions;
    }

    private String buildForeignKeyDefinitionsForTables(Connection connection,
                                                       List<ForeignKeyViolation> violations) {
        Set<String> violatingTables = violations.stream()
                .map(v -> v.violatingTable)
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        if (violatingTables.isEmpty()) return "";

        StringBuilder sb = new StringBuilder("\n\nDefinições de FKs para inspeção:\n");
        for (String table : violatingTables) {
            sb.append(" - ").append(table).append(":\n");
            String sql = String.format(PRAGMA_FK_LIST, quoteIdentifier(table));
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    int id       = safeGetInt(rs, "id");
                    int seq      = safeGetInt(rs, "seq");
                    String parent= safeGetString(rs, "table");
                    String from  = safeGetString(rs, "from");
                    String to    = safeGetString(rs, "to");
                    String onUpd = safeGetString(rs, "on_update");
                    String onDel = safeGetString(rs, "on_delete");
                    String match = safeGetString(rs, "match");

                    sb.append("    id=").append(id)
                            .append(" seq=").append(seq)
                            .append(" from=").append(from)
                            .append(" -> ").append(parent).append("(").append(to).append(")")
                            .append(" on_update=").append(onUpd)
                            .append(" on_delete=").append(onDel)
                            .append(" match=").append(match)
                            .append('\n');
                }
            } catch (SQLException e) {
                sb.append("    (falha ao ler foreign_key_list: ").append(e.getMessage()).append(")\n");
            }
        }
        return sb.toString();
    }

    // ===================== Utilidades de Execução e Estado =====================

    private static void executeStatement(Connection connection, String sql) throws SQLException {
        try (Statement st = connection.createStatement()) {
            st.execute(sql);
        }
    }

    private static void safelyExecute(Connection connection, String sql) {
        try (Statement st = connection.createStatement()) {
            st.execute(sql);
        } catch (SQLException ignored) { }
    }

    private static void safeRollback(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignored) { }
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static String safeGetString(ResultSet rs, String column) {
        try {
            String value = rs.getString(column);
            return value == null ? "" : value;
        } catch (SQLException e) {
            return "";
        }
    }

    private static int safeGetInt(ResultSet rs, String column) {
        try {
            return rs.getInt(column);
        } catch (SQLException e) {
            return -1;
        }
    }

    private static long safeGetLong(ResultSet rs, String column) {
        try {
            return rs.getLong(column);
        } catch (SQLException e) {
            return -1L;
        }
    }

    // ===================== Preservação de AUTOINCREMENT =====================

    /**
     * Lê a DDL original da tabela a partir de sqlite_master.sql.
     */
    private static String readCreateTableSql(Connection connection, String tableName) throws SQLException {
        String createSql = null;
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?")) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) createSql = rs.getString(1);
            }
        }
        return createSql == null ? "" : createSql;
    }

    /**
     * Detecta colunas com AUTOINCREMENT na DDL original.
     * Heurística segura para SQLite: só é válido quando a coluna é "INTEGER PRIMARY KEY AUTOINCREMENT".
     */
    private static Set<String> detectAutoIncrementColumns(String createTableSql,
                                                          Collection<String> candidatePrimaryKeyColumns) {
        Set<String> autoIncrement = new HashSet<>();
        if (createTableSql == null || createTableSql.isBlank()) return autoIncrement;

        String ddlUpper = createTableSql.toUpperCase(Locale.ROOT);

        if (!ddlUpper.contains("AUTOINCREMENT")) return autoIncrement;

        for (String columnName : candidatePrimaryKeyColumns) {
            // Procuramos tokens básicos na DDL original
            String quotedUpper = "\"" + columnName.replace("\"", "\"\"").toUpperCase(Locale.ROOT) + "\"";
            boolean appears     = ddlUpper.contains(quotedUpper);
            boolean isInteger   = ddlUpper.contains("INTEGER");
            boolean isPk        = ddlUpper.contains("PRIMARY KEY");
            boolean hasAutoInc  = ddlUpper.contains("AUTOINCREMENT");

            if (appears && isInteger && isPk && hasAutoInc) {
                autoIncrement.add(columnName);
            }
        }
        return autoIncrement;
    }
}
