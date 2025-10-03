package com.github.jhonatas48.migrationapi.core.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Rebuilder de tabelas SQLite para aplicar alterações de FKs preservando:
 * - Colunas (NOT NULL, DEFAULT), PK (simples/composta) e AUTOINCREMENT (via sqlite_master)
 * - Índices e triggers (via sqlite_master)
 * - Nomes FÍSICOS de tabelas e colunas (normaliza camelCase ↔ snake_case)
 *
 * Fluxo:
 *  1) PRAGMA foreign_keys=OFF + legacy_alter_table=ON
 *  2) Limpa resíduos __tmp_* e __bak_*
 *  3) Lê schema físico e sqlite_master (DDL, índices, triggers)
 *  4) Calcula FKs finais + normaliza TABELAS e COLUNAS
 *  5) Cria tabela temporária com schema final
 *  6) Copia dados
 *  7) RENAME original→__bak_ e temp→original
 *  8) DROP __bak_
 *  9) Recria índices e triggers
 * 10) PRAGMA foreign_keys=ON + PRAGMA foreign_key_check
 */
public class SqliteTableRebuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqliteTableRebuilder.class);

    // PRAGMAs / SQL utilitários
    private static final String PRAGMA_FOREIGN_KEYS_ON    = "PRAGMA foreign_keys=ON";
    private static final String PRAGMA_FOREIGN_KEYS_OFF   = "PRAGMA foreign_keys=OFF";
    private static final String PRAGMA_FOREIGN_KEYS_STATE = "PRAGMA foreign_keys";
    private static final String PRAGMA_TABLE_INFO         = "PRAGMA table_info(%s)";
    private static final String PRAGMA_INDEX_LIST         = "PRAGMA index_list(%s)";
    private static final String PRAGMA_INDEX_INFO         = "PRAGMA index_info(%s)";
    private static final String PRAGMA_FK_LIST            = "PRAGMA foreign_key_list(%s)";
    private static final String PRAGMA_FK_CHECK           = "PRAGMA foreign_key_check";
    private static final String PRAGMA_LEGACY_ALTER_ON    = "PRAGMA legacy_alter_table=ON";

    private static final String TEMP_TABLE_PREFIX   = "__tmp_";
    private static final String BACKUP_TABLE_PREFIX = "__bak_";

    /** Especificação imutável de uma FK. */
    public static final class ForeignKeySpec {
        private final String baseColumnsCsv;
        private final String referencedTable;
        private final String referencedColumnsCsv;
        private final String onDeleteAction;
        private final String onUpdateAction;
        private final String matchAction;

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

        public ForeignKeySpec withReferencedTable(String newTable) {
            return new ForeignKeySpec(baseColumnsCsv, newTable, referencedColumnsCsv, onDeleteAction, onUpdateAction, matchAction);
        }
        public ForeignKeySpec withBaseColumnsCsv(String newBaseCsv) {
            return new ForeignKeySpec(newBaseCsv, referencedTable, referencedColumnsCsv, onDeleteAction, onUpdateAction, matchAction);
        }
        public ForeignKeySpec withReferencedColumnsCsv(String newRefCsv) {
            return new ForeignKeySpec(baseColumnsCsv, referencedTable, newRefCsv, onDeleteAction, onUpdateAction, matchAction);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(baseColumnsCsv).append(" -> ").append(referencedTable)
                    .append("(").append(referencedColumnsCsv).append(")");
            if (onDeleteAction != null && !onDeleteAction.isBlank()) sb.append(" ON DELETE ").append(onDeleteAction);
            if (onUpdateAction != null && !onUpdateAction.isBlank()) sb.append(" ON UPDATE ").append(onUpdateAction);
            if (matchAction != null && !matchAction.isBlank() && !"NONE".equalsIgnoreCase(matchAction)) {
                sb.append(" MATCH ").append(matchAction);
            }
            return sb.toString();
        }
    }

    private final DataSource dataSource;

    public SqliteTableRebuilder(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    // ========================================================================
    // Orquestração principal
    // ========================================================================

    public void rebuildTableApplyingForeignKeyChanges(String tableName,
                                                      List<ForeignKeySpec> foreignKeysToAdd,
                                                      List<ForeignKeySpec> foreignKeysToDrop) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            final boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            try {
                disableForeignKeyEnforcement(connection);
                safelyExecute(connection, PRAGMA_LEGACY_ALTER_ON);

                // Limpa resíduos
                dropTableIfExists(connection, TEMP_TABLE_PREFIX + tableName);
                dropTableIfExists(connection, BACKUP_TABLE_PREFIX + tableName);

                // Schema físico atual
                TableSchema currentSchema = readCurrentTableSchema(connection, tableName);
                if (currentSchema.columns.isEmpty()) {
                    throw new IllegalStateException("Tabela não encontrada ou sem colunas: " + tableName);
                }

                // DDL/índices/triggers originais
                String originalCreateTableSql = readCreateTableSql(connection, tableName);
                List<RawIndexDef> originalIndexesSql = readIndexCreateSql(connection, tableName);
                List<RawTriggerDef> originalTriggersSql = readTriggerCreateSql(connection, tableName);

                // PK e AUTOINCREMENT
                List<String> currentPkColumns = currentSchema.columns.stream()
                        .filter(c -> c.partOfPrimaryKey)
                        .map(c -> c.name)
                        .toList();
                Set<String> autoIncrementColumns = currentPkColumns.size() == 1
                        ? detectAutoIncrementColumns(originalCreateTableSql, currentPkColumns)
                        : Collections.emptySet();

                // FKs atuais + plano final (add/drop)
                List<ForeignKeySpec> currentFks = readCurrentForeignKeys(connection, tableName);
                List<ForeignKeySpec> desiredFks = computeFinalForeignKeys(currentFks, foreignKeysToAdd, foreignKeysToDrop);

                // Normaliza tabelas referenciadas
                Set<String> existingTables = readExistingTableNames(connection);
                desiredFks = normalizeReferencedTablesOrFail(desiredFks, existingTables);

                // Normaliza colunas (base e referenciadas)
                desiredFks = normalizeForeignKeyColumnsOrFail(connection, tableName, currentSchema, desiredFks);

                // Cria temp e copia dados
                String tempTableName   = TEMP_TABLE_PREFIX + tableName;
                String backupTableName = BACKUP_TABLE_PREFIX + tableName;
                String createTempSql   = buildCreateTableStatement(tableName, currentSchema, desiredFks, tempTableName, autoIncrementColumns);

                createTemporaryTable(connection, createTempSql);
                copyDataSameColumns(connection, tableName, tempTableName, currentSchema);

                // Swap seguro
                renameTableSafely(connection, tableName, backupTableName);
                renameTableSafely(connection, tempTableName, tableName);

                // Remove backup
                dropTableSafely(connection, backupTableName);

                // Recria índices e triggers
                recreateIndexesFromSqlMaster(connection, tableName, originalIndexesSql);
                recreateTriggersFromSqlMaster(connection, tableName, originalTriggersSql);

                // Validação
                enableForeignKeyEnforcement(connection);
                validateReferentialIntegrityOrFail(connection, tableName);

                connection.commit();
                LOGGER.info("Rebuild da tabela '{}' concluído com sucesso.", tableName);
            } catch (Throwable failure) {
                safeRollback(connection);
                safelyExecute(connection, PRAGMA_FOREIGN_KEYS_ON);
                throw failure;
            } finally {
                safelyExecute(connection, PRAGMA_FOREIGN_KEYS_ON);
                connection.setAutoCommit(previousAutoCommit);
            }
        }
    }

    // ========================================================================
    // MÉTODOS QUE ESTAVAM FALTANDO
    // ========================================================================

    /** Desliga a checagem de FKs durante o rebuild. */
    private void disableForeignKeyEnforcement(Connection connection) throws SQLException {
        executeStatement(connection, PRAGMA_FOREIGN_KEYS_OFF);
    }

    /** Religa a checagem de FKs após o rebuild. */
    private void enableForeignKeyEnforcement(Connection connection) throws SQLException {
        executeStatement(connection, PRAGMA_FOREIGN_KEYS_ON);
    }

    /**
     * Calcula o conjunto final de FKs:
     * - Parte das atuais
     * - Remove as pedidas em drop (match por colunas base OU alvo referenciado)
     * - Adiciona as pedidas em add
     */
    private List<ForeignKeySpec> computeFinalForeignKeys(List<ForeignKeySpec> currentForeignKeys,
                                                         List<ForeignKeySpec> foreignKeysToAdd,
                                                         List<ForeignKeySpec> foreignKeysToDrop) {
        List<ForeignKeySpec> result = new ArrayList<>(Optional.ofNullable(currentForeignKeys).orElseGet(List::of));
        List<ForeignKeySpec> drops  = Optional.ofNullable(foreignKeysToDrop).orElseGet(List::of);
        List<ForeignKeySpec> adds   = Optional.ofNullable(foreignKeysToAdd).orElseGet(List::of);

        for (ForeignKeySpec toDrop : drops) {
            result.removeIf(existing ->
                    sameByBaseColumns(existing, toDrop) || sameByReferencedTarget(existing, toDrop)
            );
        }
        result.addAll(adds);
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
        if (csv == null) return "";
        return Arrays.stream(csv.split(",")).map(String::trim).collect(Collectors.joining(","));
    }

    // ========================================================================
    // Normalização de TABELAS e COLUNAS
    // ========================================================================

    private Set<String> readExistingTableNames(Connection connection) throws SQLException {
        Set<String> tableNames = new LinkedHashSet<>();
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) tableNames.add(rs.getString(1));
            }
        }
        return tableNames;
    }

    private List<ForeignKeySpec> normalizeReferencedTablesOrFail(List<ForeignKeySpec> desiredFks,
                                                                 Set<String> existingTables) {
        if (desiredFks == null || desiredFks.isEmpty()) return desiredFks;

        Map<String, String> byLower = existingTables.stream()
                .collect(Collectors.toMap(s -> s.toLowerCase(Locale.ROOT), s -> s, (a, b) -> a, LinkedHashMap::new));

        Map<String, String> byCanonical = new LinkedHashMap<>();
        for (String t : existingTables) byCanonical.put(canonical(t), t);

        List<ForeignKeySpec> normalized = new ArrayList<>(desiredFks.size());
        for (ForeignKeySpec fk : desiredFks) {
            String ref = fk.getReferencedTable();
            if (ref == null || ref.isBlank()) { normalized.add(fk); continue; }

            if (existingTables.contains(ref)) { normalized.add(fk); continue; }

            String lowerMatch = byLower.get(ref.toLowerCase(Locale.ROOT));
            if (lowerMatch != null) {
                if (!lowerMatch.equals(ref)) LOGGER.info("Normalizando tabela referenciada '{}' -> '{}'", ref, lowerMatch);
                normalized.add(fk.withReferencedTable(lowerMatch));
                continue;
            }

            String canonicalMatch = byCanonical.get(canonical(ref));
            if (canonicalMatch != null) {
                LOGGER.info("Normalizando (canônico) '{}' -> '{}'", ref, canonicalMatch);
                normalized.add(fk.withReferencedTable(canonicalMatch));
                continue;
            }

            String snakeGuess = camelToUnderscore(ref);
            String snakeMatch = byLower.get(snakeGuess.toLowerCase(Locale.ROOT));
            if (snakeMatch != null) {
                LOGGER.info("Normalizando (camel->underscore) '{}' -> '{}'", ref, snakeMatch);
                normalized.add(fk.withReferencedTable(snakeMatch));
                continue;
            }

            String hint = existingTables.stream().sorted().collect(Collectors.joining(", "));
            throw new IllegalStateException(
                    "Tabela referenciada pela FK não existe: '" + ref + "'. " +
                            "Tabelas existentes: [" + hint + "]"
            );
        }
        return normalized;
    }

    private List<ForeignKeySpec> normalizeForeignKeyColumnsOrFail(Connection connection,
                                                                  String baseTableName,
                                                                  TableSchema baseTableSchema,
                                                                  List<ForeignKeySpec> desiredFks) throws SQLException {
        Set<String> basePhysicalColumns = baseTableSchema.columns.stream()
                .map(c -> c.name)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Map<String, Set<String>> referencedTableToColumns = new HashMap<>();

        List<ForeignKeySpec> normalized = new ArrayList<>(desiredFks.size());
        for (ForeignKeySpec fk : desiredFks) {
            // Base
            String normalizedBaseCsv = normalizeColumnsCsvOrFail(
                    fk.getBaseColumnsCsv(),
                    basePhysicalColumns,
                    () -> "FK base (" + baseTableName + ")"
            );

            // Referenciadas
            String refTable = fk.getReferencedTable();
            Set<String> refPhysicalColumns = referencedTableToColumns.computeIfAbsent(refTable, t -> {
                try { return readPhysicalColumnNames(connection, t); }
                catch (SQLException e) { throw new RuntimeException(e); }
            });

            String normalizedRefCsv = normalizeColumnsCsvOrFail(
                    fk.getReferencedColumnsCsv(),
                    refPhysicalColumns,
                    () -> "FK referenciada (" + refTable + ")"
            );

            normalized.add(fk.withBaseColumnsCsv(normalizedBaseCsv)
                    .withReferencedColumnsCsv(normalizedRefCsv));
        }
        return normalized;
    }

    private static Set<String> readPhysicalColumnNames(Connection connection, String tableName) throws SQLException {
        Set<String> columns = new LinkedHashSet<>();
        String pragma = String.format(PRAGMA_TABLE_INFO, quoteIdentifier(tableName));
        try (PreparedStatement ps = connection.prepareStatement(pragma);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) columns.add(rs.getString("name"));
        }
        return columns;
    }

    private static String normalizeColumnsCsvOrFail(String csv,
                                                    Set<String> physicalColumns,
                                                    Supplier<String> contextSupplier) {
        if (csv == null || csv.isBlank()) return csv;

        Map<String, String> byLower = physicalColumns.stream()
                .collect(Collectors.toMap(s -> s.toLowerCase(Locale.ROOT), s -> s, (a, b) -> a, LinkedHashMap::new));

        Map<String, String> byCanonical = new LinkedHashMap<>();
        for (String c : physicalColumns) byCanonical.put(canonical(c), c);

        List<String> resolved = new ArrayList<>();
        for (String raw : csv.split(",")) {
            String candidate = raw.trim();
            if (candidate.isEmpty()) continue;

            if (physicalColumns.contains(candidate)) { resolved.add(candidate); continue; }
            String lower = byLower.get(candidate.toLowerCase(Locale.ROOT));
            if (lower != null) { resolved.add(lower); continue; }
            String canonical = byCanonical.get(canonical(candidate));
            if (canonical != null) { resolved.add(canonical); continue; }
            String snakeGuess = camelToUnderscore(candidate);
            String snake = byLower.get(snakeGuess.toLowerCase(Locale.ROOT));
            if (snake != null) { resolved.add(snake); continue; }

            String ctx = contextSupplier.get();
            String hint = String.join(", ", physicalColumns);
            throw new IllegalStateException("Coluna não encontrada " + ctx + ": '" + candidate + "'. Existentes: [" + hint + "]");
        }

        return String.join(",", resolved);
    }

    // ========================================================================
    // Construção do CREATE TABLE
    // ========================================================================

    private String buildCreateTableStatement(String originalTableName,
                                             TableSchema schema,
                                             List<ForeignKeySpec> foreignKeys,
                                             String tempTableName,
                                             Set<String> autoIncrementColumns) {
        StringBuilder create = new StringBuilder();
        create.append("CREATE TABLE ").append(quoteIdentifier(tempTableName)).append(" (\n");

        List<String> primaryKeyColumns = schema.columns.stream()
                .filter(c -> c.partOfPrimaryKey)
                .map(c -> c.name)
                .toList();

        List<String> definitionParts = new ArrayList<>();

        for (TableColumn column : schema.columns) {
            definitionParts.add(buildColumnDefinition(primaryKeyColumns, column, autoIncrementColumns));
        }

        if (primaryKeyColumns.size() > 1) {
            String pkList = primaryKeyColumns.stream().map(SqliteTableRebuilder::quoteIdentifier).collect(Collectors.joining(", "));
            definitionParts.add("  PRIMARY KEY (" + pkList + ")");
        }

        for (ForeignKeySpec fk : foreignKeys) {
            if (fk.getReferencedTable() == null || fk.getReferencedTable().isBlank()) continue;
            definitionParts.add(buildForeignKeyDefinition(fk));
        }

        create.append(String.join(",\n", definitionParts)).append("\n)");
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
            boolean isIntegerType = column.typeDeclaration != null && column.typeDeclaration.trim().equalsIgnoreCase("INTEGER");
            if (isIntegerType && autoIncrementColumns.contains(column.name)) {
                definition.append(" AUTOINCREMENT");
            }
        }
        if (column.notNull) definition.append(" NOT NULL");
        if (column.defaultExpression != null) definition.append(" DEFAULT ").append(column.defaultExpression);
        return definition.toString();
    }

    private String buildForeignKeyDefinition(ForeignKeySpec fk) {
        String baseList = Arrays.stream(fk.getBaseColumnsCsv().split(","))
                .map(String::trim).map(SqliteTableRebuilder::quoteIdentifier).collect(Collectors.joining(", "));
        String refList = Arrays.stream(fk.getReferencedColumnsCsv().split(","))
                .map(String::trim).map(SqliteTableRebuilder::quoteIdentifier).collect(Collectors.joining(", "));

        StringBuilder sb = new StringBuilder();
        sb.append("  FOREIGN KEY (").append(baseList).append(")")
                .append(" REFERENCES ").append(quoteIdentifier(fk.getReferencedTable()))
                .append(" (").append(refList).append(")");

        if (fk.getOnDelete() != null && !fk.getOnDelete().isBlank()) sb.append(" ON DELETE ").append(fk.getOnDelete());
        if (fk.getOnUpdate() != null && !fk.getOnUpdate().isBlank()) sb.append(" ON UPDATE ").append(fk.getOnUpdate());
        if (fk.getMatch() != null && !fk.getMatch().isBlank() && !"NONE".equalsIgnoreCase(fk.getMatch())) {
            sb.append(" MATCH ").append(fk.getMatch());
        }
        return sb.toString();
    }

    // ========================================================================
    // Leitura de metadados
    // ========================================================================

    private TableSchema readCurrentTableSchema(Connection connection, String tableName) throws SQLException {
        TableSchema schema = new TableSchema();
        String sql = String.format(PRAGMA_TABLE_INFO, quoteIdentifier(tableName));
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String name   = rs.getString("name");
                String type   = rs.getString("type");
                boolean nn    = rs.getInt("notnull") == 1;
                String dflt   = rs.getString("dflt_value");
                boolean isPk  = rs.getInt("pk") == 1;
                schema.columns.add(new TableColumn(name, type, nn, dflt, isPk));
            }
        }
        return schema;
    }

    private List<ForeignKeySpec> readCurrentForeignKeys(Connection connection, String tableName) throws SQLException {
        Map<Integer, List<ForeignKeyRow>> groupedByConstraint = new LinkedHashMap<>();
        String sql = String.format(PRAGMA_FK_LIST, quoteIdentifier(tableName));
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                int constraintId  = rs.getInt("id");
                String refTable   = rs.getString("table");
                String fromColumn = rs.getString("from");
                String toColumn   = rs.getString("to");
                String onUpdate   = rs.getString("on_update");
                String onDelete   = rs.getString("on_delete");
                String match      = rs.getString("match");
                groupedByConstraint.computeIfAbsent(constraintId, k -> new ArrayList<>())
                        .add(new ForeignKeyRow(refTable, fromColumn, toColumn, onUpdate, onDelete, match));
            }
        }

        List<ForeignKeySpec> fks = new ArrayList<>();
        for (List<ForeignKeyRow> rows : groupedByConstraint.values()) {
            String refTable = rows.get(0).referencedTable;
            String onUpd    = rows.get(0).onUpdateAction;
            String onDel    = rows.get(0).onDeleteAction;
            String match    = rows.get(0).matchAction;
            String baseCsv  = rows.stream().map(r -> r.baseColumn).collect(Collectors.joining(","));
            String refCsv   = rows.stream().map(r -> r.referencedColumn).collect(Collectors.joining(","));
            fks.add(new ForeignKeySpec(baseCsv, refTable, refCsv, onDel, onUpd, match));
        }
        return fks;
    }

    private List<RawIndexDef> readIndexCreateSql(Connection connection, String tableName) throws SQLException {
        List<RawIndexDef> out = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL")) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(new RawIndexDef(rs.getString("name"), rs.getString("sql")));
            }
        }
        return out;
    }

    private List<RawTriggerDef> readTriggerCreateSql(Connection connection, String tableName) throws SQLException {
        List<RawTriggerDef> out = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?")) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(new RawTriggerDef(rs.getString("name"), rs.getString("sql")));
            }
        }
        return out;
    }

    // ========================================================================
    // Diagnóstico de integridade
    // ========================================================================

    private List<ForeignKeyViolation> runForeignKeyCheck(Connection connection) {
        List<ForeignKeyViolation> violations = new ArrayList<>();
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(PRAGMA_FK_CHECK)) {
            while (rs.next()) {
                String table  = safeGetString(rs, "table");
                long rowId    = safeGetLong(rs, "rowid");
                String parent = safeGetString(rs, "parent");
                int fkid      = safeGetInt(rs, "fkid");
                violations.add(new ForeignKeyViolation(table, rowId, parent, fkid));
            }
        } catch (SQLException e) {
            LOGGER.warn("Falha ao executar PRAGMA foreign_key_check: {}", e.getMessage());
        }
        return violations;
    }

    private void validateReferentialIntegrityOrFail(Connection connection, String rebuiltTableName) throws SQLException {
        List<ForeignKeyViolation> violations = runForeignKeyCheck(connection);
        if (violations.isEmpty()) return;

        String message = buildForeignKeyViolationMessage(rebuiltTableName, violations, connection);
        LOGGER.error(message);
        throw new IllegalStateException(message);
    }

    private String buildForeignKeyViolationMessage(String rebuiltTableName,
                                                   List<ForeignKeyViolation> violations,
                                                   Connection connection) {
        String header = "Violação(ões) de integridade referencial após rebuild da tabela '" + rebuiltTableName + "'.";
        String details = violations.stream()
                .map(v -> "table=" + v.violatingTable + ", rowid=" + v.violatingRowId + ", parent=" + v.referencedParentTable + ", fkid=" + v.foreignKeyId)
                .collect(Collectors.joining("\n"));
        String definitions = buildForeignKeyDefinitionsForTables(connection, violations);
        return header + "\n" + details + definitions;
    }

    private String buildForeignKeyDefinitionsForTables(Connection connection, List<ForeignKeyViolation> violations) {
        Set<String> violatingTables = violations.stream()
                .map(v -> v.violatingTable)
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        if (violatingTables.isEmpty()) return "";

        StringBuilder sb = new StringBuilder("\n\nDefinições de FKs para inspeção:\n");
        for (String table : violatingTables) {
            sb.append(" - ").append(table).append(":\n");
            String sql = String.format(PRAGMA_FK_LIST, quoteIdentifier(table));
            try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    int id = safeGetInt(rs, "id");
                    int seq = safeGetInt(rs, "seq");
                    String parent = safeGetString(rs, "table");
                    String from = safeGetString(rs, "from");
                    String to = safeGetString(rs, "to");
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

    // ========================================================================
    // Helpers de swap / drop / execução
    // ========================================================================

    private void renameTableSafely(Connection connection, String fromTable, String toTable) throws SQLException {
        boolean wasOn = isForeignKeysOn(connection);
        if (wasOn) setForeignKeys(connection, false);
        try {
            executeStatement(connection, "ALTER TABLE " + quoteIdentifier(fromTable) + " RENAME TO " + quoteIdentifier(toTable));
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

    private void recreateIndexesFromSqlMaster(Connection connection, String tableName, List<RawIndexDef> indexDefs) throws SQLException {
        for (RawIndexDef def : indexDefs) {
            if (def.createSql == null || def.createSql.isBlank()) continue; // índices implícitos (PK)
            executeStatement(connection, def.createSql);
        }
    }

    private void recreateTriggersFromSqlMaster(Connection connection, String tableName, List<RawTriggerDef> triggerDefs) throws SQLException {
        for (RawTriggerDef def : triggerDefs) {
            if (def.createSql == null || def.createSql.isBlank()) continue;
            executeStatement(connection, def.createSql);
        }
    }

    private void dropTableIfExists(Connection connection, String tableName) throws SQLException {
        executeStatement(connection, "DROP TABLE IF EXISTS " + quoteIdentifier(tableName));
    }

    private static void executeStatement(Connection connection, String sql) throws SQLException {
        try (Statement st = connection.createStatement()) {
            st.execute(sql);
        }
    }

    private static void safelyExecute(Connection connection, String sql) {
        try (Statement st = connection.createStatement()) {
            st.execute(sql);
        } catch (SQLException ignored) {}
    }

    private static boolean isForeignKeysOn(Connection connection) {
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(PRAGMA_FOREIGN_KEYS_STATE)) {
            if (rs.next()) return rs.getInt(1) == 1;
        } catch (SQLException ignored) {}
        return false;
    }

    private static void setForeignKeys(Connection connection, boolean on) {
        safelyExecute(connection, on ? PRAGMA_FOREIGN_KEYS_ON : PRAGMA_FOREIGN_KEYS_OFF);
    }

    private static void safeRollback(Connection connection) {
        try { connection.rollback(); } catch (SQLException ignored) {}
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static String safeGetString(ResultSet rs, String column) {
        try { String v = rs.getString(column); return v == null ? "" : v; } catch (SQLException e) { return ""; }
    }
    private static int safeGetInt(ResultSet rs, String column) {
        try { return rs.getInt(column); } catch (SQLException e) { return -1; }
    }
    private static long safeGetLong(ResultSet rs, String column) {
        try { return rs.getLong(column); } catch (SQLException e) { return -1L; }
    }

    // ========================================================================
    // AUTOINCREMENT (sqlite_master)
    // ========================================================================

    private static String readCreateTableSql(Connection connection, String tableName) throws SQLException {
        String sql = null;
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?")) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) sql = rs.getString(1);
            }
        }
        return sql == null ? "" : sql;
    }

    private static Set<String> detectAutoIncrementColumns(String createTableSql,
                                                          Collection<String> candidatePkColumns) {
        Set<String> autoInc = new HashSet<>();
        if (createTableSql == null || createTableSql.isBlank()) return autoInc;

        String ddlUpper = createTableSql.toUpperCase(Locale.ROOT);
        if (!ddlUpper.contains("AUTOINCREMENT")) return autoInc;

        for (String col : candidatePkColumns) {
            String quotedUpper = "\"" + col.replace("\"", "\"\"").toUpperCase(Locale.ROOT) + "\"";
            boolean appears    = ddlUpper.contains(quotedUpper);
            boolean isInteger  = ddlUpper.contains("INTEGER");
            boolean isPk       = ddlUpper.contains("PRIMARY KEY");
            boolean hasAuto    = ddlUpper.contains("AUTOINCREMENT");
            if (appears && isInteger && isPk && hasAuto) autoInc.add(col);
        }
        return autoInc;
    }

    // ========================================================================
    // Modelos internos
    // ========================================================================

    private static final class TableSchema {
        private final List<TableColumn> columns = new ArrayList<>();
    }

    private static final class TableColumn {
        private final String name;
        private final String typeDeclaration;
        private final boolean notNull;
        private final String defaultExpression;
        private final boolean partOfPrimaryKey;

        private TableColumn(String name, String type, boolean notNull, String defaultExpr, boolean partOfPrimaryKey) {
            this.name = name;
            this.typeDeclaration = type;
            this.notNull = notNull;
            this.defaultExpression = defaultExpr;
            this.partOfPrimaryKey = partOfPrimaryKey;
        }
    }

    private static final class ForeignKeyRow {
        private final String referencedTable;
        private final String baseColumn;
        private final String referencedColumn;
        private final String onUpdateAction;
        private final String onDeleteAction;
        private final String matchAction;

        private ForeignKeyRow(String referencedTable, String baseColumn, String referencedColumn,
                              String onUpdateAction, String onDeleteAction, String matchAction) {
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

        private ForeignKeyViolation(String table, long rowId, String parent, int fkId) {
            this.violatingTable = table;
            this.violatingRowId = rowId;
            this.referencedParentTable = parent;
            this.foreignKeyId = fkId;
        }
    }

    private static final class RawIndexDef {
        final String name;
        final String createSql;
        RawIndexDef(String name, String createSql) { this.name = name; this.createSql = createSql; }
    }

    private static final class RawTriggerDef {
        final String name;
        final String createSql;
        RawTriggerDef(String name, String createSql) { this.name = name; this.createSql = createSql; }
    }

    // ========================================================================
    // Utilidades de nome
    // ========================================================================

    private static String canonical(String name) {
        if (name == null) return "";
        return name.replaceAll("[^A-Za-z0-9]", "").toLowerCase(Locale.ROOT);
    }

    /** camelCase → snake_case simples (insere '_' antes de maiúsculas, exceto a primeira). */
    private static String camelToUnderscore(String name) {
        if (name == null || name.isBlank()) return name;
        StringBuilder sb = new StringBuilder();
        char[] chars = name.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            char ch = chars[i];
            if (i > 0 && Character.isUpperCase(ch) &&
                    (Character.isLowerCase(chars[i - 1]) || Character.isDigit(chars[i - 1]))) {
                sb.append('_');
            }
            sb.append(ch);
        }
        return sb.toString();
    }
}
