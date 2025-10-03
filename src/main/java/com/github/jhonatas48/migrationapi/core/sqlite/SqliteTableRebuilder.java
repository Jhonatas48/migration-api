package com.github.jhonatas48.migrationapi.core.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Reconstrói tabelas no SQLite para aplicar alterações de FKs preservando:
 * - NOT NULL, DEFAULT, PK (single/composta) e AUTOINCREMENT (via sqlite_master)
 * - ÍNDICES (incl. parciais/expressões/collation) e TRIGGERS (via sqlite_master)
 *
 * Fluxo robusto:
 *  1) PRAGMA foreign_keys = OFF + legacy_alter_table=ON
 *  2) Resolve nome físico da tabela (camel/snake/case-insensitive)
 *  3) Limpa resíduos (__tmp_* e __bak_*)
 *  4) Lê schema corrente (PRAGMAs) e DDL original (sqlite_master.sql)
 *  5) Calcula FKs finais (inclui MATCH) e normaliza nomes das tabelas referenciadas
 *  6) Cria tabela temporária com schema final
 *  7) Copia dados coluna-a-coluna (mesmo conjunto)
 *  8) SWAP: RENAME original -> __bak_, RENAME temp -> original
 *  9) DROP __bak_ (seguro)
 * 10) Recria índices e triggers pela DDL do sqlite_master
 * 11) PRAGMA foreign_keys = ON + PRAGMA foreign_key_check (diagnóstico)
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

    /** Especificação de uma FK (colunas base e referenciadas + ações). */
    public static final class ForeignKeySpec {
        private final String baseColumnsCsv;
        private final String referencedTable;
        private final String referencedColumnsCsv;
        private final String onDeleteAction;
        private final String onUpdateAction;
        private final String matchAction; // opcional (SIMPLE/FULL/PARTIAL/NONE)

        public ForeignKeySpec(String baseColumnsCsv,
                              String referencedTable,
                              String referencedColumnsCsv,
                              String onDelete,
                              String onUpdate,
                              String matchAction) {
            this.baseColumnsCsv = baseColumnsCsv;
            this.referencedTable = referencedTable;
            this.referencedColumnsCsv = referencedColumnsCsv;
            this.onDeleteAction = onDelete;
            this.onUpdateAction = onUpdate;
            this.matchAction = matchAction;
        }

        public String getBaseColumnsCsv()       { return baseColumnsCsv; }
        public String getReferencedTable()      { return referencedTable; }
        public String getReferencedColumnsCsv() { return referencedColumnsCsv; }
        public String getOnDelete()             { return onDeleteAction; }
        public String getOnUpdate()             { return onUpdateAction; }
        public String getMatch()                { return matchAction; }

        public ForeignKeySpec withReferencedTable(String newReferencedTable) {
            return new ForeignKeySpec(baseColumnsCsv, newReferencedTable, referencedColumnsCsv, onDeleteAction, onUpdateAction, matchAction);
        }

        @Override
        public String toString() {
            StringBuilder description = new StringBuilder();
            description.append(baseColumnsCsv)
                    .append(" -> ")
                    .append(referencedTable)
                    .append("(").append(referencedColumnsCsv).append(")");
            if (onDeleteAction != null && !onDeleteAction.isBlank()) description.append(" ON DELETE ").append(onDeleteAction);
            if (onUpdateAction != null && !onUpdateAction.isBlank()) description.append(" ON UPDATE ").append(onUpdateAction);
            if (matchAction != null && !matchAction.isBlank() && !"NONE".equalsIgnoreCase(matchAction)) {
                description.append(" MATCH ").append(matchAction);
            }
            return description.toString();
        }
    }

    private final DataSource dataSource;

    public SqliteTableRebuilder(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Reconstrói a tabela para aplicar adições/remoções de FKs preservando colunas, índices e triggers.
     */
    public void rebuildTableApplyingForeignKeyChanges(String logicalTableName,
                                                      List<ForeignKeySpec> foreignKeysToAdd,
                                                      List<ForeignKeySpec> foreignKeysToDrop) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            try {
                // 1) FK OFF + facilitadores de ALTER em versões antigas
                disableForeignKeyEnforcement(connection);
                safelyExecute(connection, PRAGMA_LEGACY_ALTER_ON);

                // 2) Resolve o NOME FÍSICO real da tabela (snake/camel/case-insensitive)
                final String physicalTableName = resolvePhysicalTableName(connection, logicalTableName);

                // 3) Limpeza de resíduos de execuções falhas usando nome físico
                dropTableIfExists(connection, TEMP_TABLE_PREFIX + physicalTableName);
                dropTableIfExists(connection, BACKUP_TABLE_PREFIX + physicalTableName);

                // 4) Metadados atuais (schema e DDL original) usando nome físico
                TableSchema currentSchema = readCurrentTableSchema(connection, physicalTableName);
                if (currentSchema.columns.isEmpty()) {
                    throw new IllegalStateException("Tabela não encontrada ou sem colunas: " + physicalTableName);
                }

                String originalCreateTableSql   = readCreateTableSql(connection, physicalTableName);
                List<RawIndexDef> indexCreateSqlCache   = readIndexCreateSql(connection, physicalTableName);
                List<RawTriggerDef> triggerCreateSqlCache = readTriggerCreateSql(connection, physicalTableName);

                // PKs atuais (para detectar AUTOINCREMENT)
                List<String> pkColumns = currentSchema.columns.stream()
                        .filter(c -> c.partOfPrimaryKey)
                        .map(c -> c.name)
                        .toList();

                Set<String> autoIncrementColumns = pkColumns.size() == 1
                        ? detectAutoIncrementColumns(originalCreateTableSql, pkColumns)
                        : Collections.emptySet();

                // 5) FKs finais (remove solicitadas, adiciona novas) + normalização de tabelas referenciadas
                List<ForeignKeySpec> currentForeignKeys = readCurrentForeignKeys(connection, physicalTableName);
                List<ForeignKeySpec> desiredForeignKeys = computeFinalForeignKeys(
                        currentForeignKeys, foreignKeysToAdd, foreignKeysToDrop
                );

                Set<String> existingTables = readExistingTableNames(connection);
                desiredForeignKeys = normalizeReferencedTablesOrFail(desiredForeignKeys, existingTables);

                // 6) Cria tabela temporária com schema final (sempre com nome físico)
                String tempTableName   = TEMP_TABLE_PREFIX + physicalTableName;
                String backupTableName = BACKUP_TABLE_PREFIX + physicalTableName;

                String createTempSql = buildCreateTableStatement(
                        physicalTableName, currentSchema, desiredForeignKeys, tempTableName, autoIncrementColumns
                );
                createTemporaryTable(connection, createTempSql);

                // 7) Copia dados coluna-a-coluna
                copyDataSameColumns(connection, physicalTableName, tempTableName, currentSchema);

                // 8) SWAP via rename com backup
                renameTableSafely(connection, physicalTableName, backupTableName); // original -> backup
                renameTableSafely(connection, tempTableName, physicalTableName);   // temp -> original

                // 9) DROP do backup (seguro, com FK OFF)
                dropTableSafely(connection, backupTableName);

                // 10) Recria índices e triggers preservando DDL original
                recreateIndexesFromSqlMaster(connection, physicalTableName, indexCreateSqlCache);
                recreateTriggersFromSqlMaster(connection, physicalTableName, triggerCreateSqlCache);

                // 11) FK ON e validação
                enableForeignKeyEnforcement(connection);
                validateReferentialIntegrityOrFail(connection, physicalTableName);

                connection.commit();
                LOGGER.info("Rebuild da tabela '{}' concluído com sucesso.", physicalTableName);
            } catch (Throwable failure) {
                safeRollback(connection);
                safelyExecute(connection, PRAGMA_FOREIGN_KEYS_ON); // garante não deixar FK OFF
                throw failure;
            } finally {
                safelyExecute(connection, PRAGMA_FOREIGN_KEYS_ON);
                connection.setAutoCommit(previousAutoCommit);
            }
        }
    }

    // ===================== Normalização de nomes (BASE e REFERENCIADAS) =====================

    /** Lista os nomes físicos das tabelas (exclui as internas sqlite_*) */
    private static Set<String> listPhysicalTables(Connection connection) throws SQLException {
        Set<String> tables = new LinkedHashSet<>();
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String n = rs.getString(1);
                    if (n != null && !n.isBlank()) tables.add(n);
                }
            }
        }
        return tables;
    }

    /** Resolve o nome físico real da tabela a partir de um nome lógico (camel/snake/case-insensitive). */
    private static String resolvePhysicalTableName(Connection connection, String requestedName) throws SQLException {
        String requested = stripQuotesIfAny(requestedName);
        Set<String> existing = listPhysicalTables(connection);

        // 1) Igualdade case-insensitive direta
        for (String t : existing) {
            if (t.equalsIgnoreCase(requested)) return t;
        }
        // 2) camel -> snake
        String snake = toSnakeCase(requested);
        for (String t : existing) {
            if (t.equalsIgnoreCase(snake)) return t;
        }
        // 3) snake -> camel
        String camel = toCamelCase(requested);
        for (String t : existing) {
            if (t.equalsIgnoreCase(camel)) return t;
        }

        String available = existing.isEmpty() ? "(nenhuma)" : String.join(", ", existing);
        throw new IllegalStateException(
                "Tabela não encontrada: " + requestedName + ". Tabelas existentes: " + available
        );
    }

    /** Normaliza a lista de FKs garantindo que as tabelas referenciadas existem fisicamente. */
    private List<ForeignKeySpec> normalizeReferencedTablesOrFail(List<ForeignKeySpec> desired,
                                                                 Set<String> existingTables) {
        if (desired == null || desired.isEmpty()) return desired;

        Map<String, String> byLower = new HashMap<>();
        Map<String, String> byCanonical = new LinkedHashMap<>();
        for (String t : existingTables) {
            byLower.put(t.toLowerCase(Locale.ROOT), t);
            byCanonical.put(canonical(t), t);
        }

        List<ForeignKeySpec> normalized = new ArrayList<>(desired.size());
        for (ForeignKeySpec fk : desired) {
            String ref = fk.getReferencedTable();
            if (ref == null || ref.isBlank()) { normalized.add(fk); continue; }

            // 1) exata
            if (existingTables.contains(ref)) {
                normalized.add(fk);
                continue;
            }
            // 2) case-insensitive
            String lowerMatch = byLower.get(ref.toLowerCase(Locale.ROOT));
            if (lowerMatch != null) {
                if (!lowerMatch.equals(ref)) {
                    LOGGER.info("Normalizando tabela referenciada '{}' -> '{}'", ref, lowerMatch);
                }
                normalized.add(fk.withReferencedTable(lowerMatch));
                continue;
            }
            // 3) canônica (remove _ e pontuação; minúscula)
            String canonicalRef = canonical(ref);
            String canonicalMatch = byCanonical.get(canonicalRef);
            if (canonicalMatch != null) {
                LOGGER.info("Normalizando (canônico) '{}' -> '{}'", ref, canonicalMatch);
                normalized.add(fk.withReferencedTable(canonicalMatch));
                continue;
            }
            // 4) heurística camel -> snake com underscore (ex.: FormDeveloper -> Form_Developer)
            String snakeGuess = camelToUnderscore(ref);
            String snakeMatch = byLower.get(snakeGuess.toLowerCase(Locale.ROOT));
            if (snakeMatch != null) {
                LOGGER.info("Normalizando (camel->underscore) '{}' -> '{}'", ref, snakeMatch);
                normalized.add(fk.withReferencedTable(snakeMatch));
                continue;
            }

            // 5) falha — lista alternativas
            String hint = existingTables.stream().sorted().collect(Collectors.joining(", "));
            throw new IllegalStateException(
                    "Tabela referenciada pela FK não existe: '" + ref + "'. " +
                            "Tabelas existentes: [" + hint + "]. " +
                            "Dica: ajuste o nome da tabela (ex.: camelCase vs. snake_case)."
            );
        }
        return normalized;
    }

    /** Converte CamelCase/pascalCase para snake_case (minúsculas). */
    private static String toSnakeCase(String name) {
        if (name == null || name.isBlank()) return name;
        return name.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase(Locale.ROOT);
    }

    /** Converte snake_case para camelCase simples. */
    private static String toCamelCase(String name) {
        if (name == null || name.isBlank()) return name;
        String[] parts = name.split("_");
        if (parts.length == 0) return name;
        StringBuilder sb = new StringBuilder(parts[0].toLowerCase(Locale.ROOT));
        for (int i = 1; i < parts.length; i++) {
            if (parts[i].isEmpty()) continue;
            sb.append(parts[i].substring(0,1).toUpperCase(Locale.ROOT))
                    .append(parts[i].substring(1).toLowerCase(Locale.ROOT));
        }
        return sb.toString();
    }

    /** Forma canônica: remove underscores e não-alfa-numéricos; minúscula. */
    private static String canonical(String name) {
        if (name == null) return "";
        return name.replaceAll("[^A-Za-z0-9]", "").toLowerCase(Locale.ROOT);
    }

    /** Heurística: insere '_' antes de maiúsculas (exceto a primeira). */
    private static String camelToUnderscore(String name) {
        if (name == null || name.isBlank()) return name;
        StringBuilder sb = new StringBuilder();
        char[] cs = name.toCharArray();
        for (int i = 0; i < cs.length; i++) {
            char ch = cs[i];
            if (i > 0 && Character.isUpperCase(ch) && (Character.isLowerCase(cs[i - 1]) || Character.isDigit(cs[i - 1]))) {
                sb.append('_');
            }
            sb.append(ch);
        }
        return sb.toString();
    }

    private static String stripQuotesIfAny(String name) {
        if (name == null) return null;
        String n = name.trim();
        if (n.startsWith("\"") && n.endsWith("\"") && n.length() >= 2) return n.substring(1, n.length() - 1);
        if (n.startsWith("'") && n.endsWith("'") && n.length() >= 2)  return n.substring(1, n.length() - 1);
        return n;
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

    private void recreateIndexesFromSqlMaster(Connection connection,
                                              String tableName,
                                              List<RawIndexDef> indexDefs) throws SQLException {
        for (RawIndexDef def : indexDefs) {
            if (def.createSql == null || def.createSql.isBlank()) continue; // índices implícitos (ex.: PK)
            executeStatement(connection, def.createSql);
        }
    }

    private void recreateTriggersFromSqlMaster(Connection connection,
                                               String tableName,
                                               List<RawTriggerDef> triggerDefs) throws SQLException {
        for (RawTriggerDef def : triggerDefs) {
            if (def.createSql == null || def.createSql.isBlank()) continue;
            executeStatement(connection, def.createSql);
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

    // ===================== Helpers de Swap/Drop/Resíduos =====================

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

    private void dropTableIfExists(Connection connection, String tableName) throws SQLException {
        executeStatement(connection, "DROP TABLE IF EXISTS " + quoteIdentifier(tableName));
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
        private final String matchAction;

        private ForeignKeyRow(String referencedTable,
                              String baseColumn,
                              String referencedColumn,
                              String onUpdateAction,
                              String onDeleteAction,
                              String matchAction) {
            this.referencedTable = referencedTable;
            this.baseColumn = baseColumn;
            this.referencedColumn = referencedColumn;
            this.onUpdateAction = onUpdateAction;
            this.onDeleteAction = onDeleteAction;
            this.matchAction = matchAction;
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

    private static final class RawIndexDef {
        final String name;
        final String createSql; // null para índices implícitos (ex.: PK)
        RawIndexDef(String name, String createSql) { this.name = name; this.createSql = createSql; }
    }

    private static final class RawTriggerDef {
        final String name;
        final String createSql; // não-null
        RawTriggerDef(String name, String createSql) { this.name = name; this.createSql = createSql; }
    }

    // ===================== Leitura de Metadados =====================

    private TableSchema readCurrentTableSchema(Connection connection, String physicalTableName) throws SQLException {
        TableSchema schema = new TableSchema();
        String sql = String.format(PRAGMA_TABLE_INFO, quoteIdentifier(physicalTableName));

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

    private List<ForeignKeySpec> readCurrentForeignKeys(Connection connection, String physicalTableName) throws SQLException {
        Map<Integer, List<ForeignKeyRow>> groupedByConstraint = new LinkedHashMap<>();
        String sql = String.format(PRAGMA_FK_LIST, quoteIdentifier(physicalTableName));

        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet result = statement.executeQuery()) {

            while (result.next()) {
                int constraintId       = result.getInt("id");
                String referencedTable  = result.getString("table");
                String baseColumn       = result.getString("from");
                String referencedColumn = result.getString("to");
                String onUpdate         = result.getString("on_update");
                String onDelete         = result.getString("on_delete");
                String matchAction      = result.getString("match");

                groupedByConstraint
                        .computeIfAbsent(constraintId, k -> new ArrayList<>())
                        .add(new ForeignKeyRow(referencedTable, baseColumn, referencedColumn, onUpdate, onDelete, matchAction));
            }
        }

        List<ForeignKeySpec> foreignKeys = new ArrayList<>();
        for (List<ForeignKeyRow> rowsOfConstraint : groupedByConstraint.values()) {
            String referencedTable      = rowsOfConstraint.get(0).referencedTable;
            String onUpdate             = rowsOfConstraint.get(0).onUpdateAction;
            String onDelete             = rowsOfConstraint.get(0).onDeleteAction;
            String matchAction          = rowsOfConstraint.get(0).matchAction;

            String baseColumnsCsv       = rowsOfConstraint.stream().map(r -> r.baseColumn).collect(Collectors.joining(","));
            String referencedColumnsCsv = rowsOfConstraint.stream().map(r -> r.referencedColumn).collect(Collectors.joining(","));

            foreignKeys.add(new ForeignKeySpec(baseColumnsCsv, referencedTable, referencedColumnsCsv, onDelete, onUpdate, matchAction));
        }

        return foreignKeys;
    }

    private List<IndexDefinition> readCurrentIndexes(Connection connection, String physicalTableName) throws SQLException {
        List<IndexDefinition> indexDefinitions = new ArrayList<>();
        String sql = String.format(PRAGMA_INDEX_LIST, quoteIdentifier(physicalTableName));

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

    private List<RawIndexDef> readIndexCreateSql(Connection connection, String physicalTableName) throws SQLException {
        List<RawIndexDef> out = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL")) {
            ps.setString(1, physicalTableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(new RawIndexDef(rs.getString("name"), rs.getString("sql")));
            }
        }
        return out;
    }

    private List<RawTriggerDef> readTriggerCreateSql(Connection connection, String physicalTableName) throws SQLException {
        List<RawTriggerDef> out = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?")) {
            ps.setString(1, physicalTableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(new RawTriggerDef(rs.getString("name"), rs.getString("sql")));
            }
        }
        return out;
    }

    // ===================== Construção do CREATE TABLE =====================

    private String buildCreateTableStatement(String physicalTableName,
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
        if (foreignKey.getMatch() != null && !foreignKey.getMatch().isBlank()
                && !"NONE".equalsIgnoreCase(foreignKey.getMatch())) {
            definition.append(" MATCH ").append(foreignKey.getMatch());
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

    /** Lê a DDL original da tabela a partir de sqlite_master.sql. */
    private static String readCreateTableSql(Connection connection, String physicalTableName) throws SQLException {
        String createSql = null;
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?")) {
            ps.setString(1, physicalTableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) createSql = rs.getString(1);
            }
        }
        return createSql == null ? "" : createSql;
    }

    /**
     * Detecta colunas com AUTOINCREMENT na DDL original.
     * Heurística suficiente: só é válido quando a coluna é "INTEGER PRIMARY KEY AUTOINCREMENT".
     */
    private static Set<String> detectAutoIncrementColumns(String createTableSql,
                                                          Collection<String> candidatePrimaryKeyColumns) {
        Set<String> autoIncrement = new HashSet<>();
        if (createTableSql == null || createTableSql.isBlank()) return autoIncrement;

        String ddlUpper = createTableSql.toUpperCase(Locale.ROOT);
        if (!ddlUpper.contains("AUTOINCREMENT")) return autoIncrement;

        for (String columnName : candidatePrimaryKeyColumns) {
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

    /** Lê nomes de todas as tabelas usuais do banco (sqlite_master). */
    private Set<String> readExistingTableNames(Connection connection) throws SQLException {
        return listPhysicalTables(connection);
    }
}
