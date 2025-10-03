package com.github.jhonatas48.migrationapi.core.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Rebuild seguro de tabelas SQLite para aplicar alterações de chaves estrangeiras preservando:
 *  - Colunas, PK (simples/composta), AUTOINCREMENT (obtido do sqlite_master)
 *  - Índices e triggers (recriadas a partir do sqlite_master)
 *  - Nomes físicos de tabelas/colunas, com normalização (case-insensitive, canonical, camel->snake)
 *
 * Fluxo:
 *  1) FK OFF + legacy_alter_table=ON
 *  2) Limpeza de resíduos __tmp_* e __bak_*
 *  3) Leitura do schema físico e sqlite_master
 *  4) Cálculo das FKs finais + normalização de tabelas/colunas
 *  5) Criação de tabela temporária com schema final
 *  6) Cópia de dados
 *  7) RENAME original→__bak_ e temp→original
 *  8) DROP __bak_
 *  9) Recriação de índices e triggers
 * 10) FK ON + foreign_key_check
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

    private final DataSource dataSource;

    public SqliteTableRebuilder(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource é obrigatório");
    }

    // =====================================================================================
    // API principal
    // =====================================================================================

    /**
     * Aplica alterações de FKs na tabela informada, preservando dados/índices/triggers e integridade.
     */
    public void rebuildTableApplyingForeignKeyChanges(
            String requestedTableName,
            List<ForeignKeySpec> foreignKeysToAdd,
            List<ForeignKeySpec> foreignKeysToDrop) throws SQLException {

        try (Connection connection = dataSource.getConnection()) {
            final boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            try {
                disableForeignKeyEnforcement(connection);
                safelyExecute(connection, PRAGMA_LEGACY_ALTER_ON);

                // Resolve nome físico da tabela-alvo e carrega tabelas existentes
                Set<String> existingTables = readExistingTableNames(connection);
                String physicalTableName = resolveExistingTableNameOrFail(requestedTableName, existingTables);

                // Limpa resíduos com base no nome físico
                dropTableIfExists(connection, TEMP_TABLE_PREFIX + physicalTableName);
                dropTableIfExists(connection, BACKUP_TABLE_PREFIX + physicalTableName);

                // Lê schema físico atual
                TableSchema currentSchema = readCurrentTableSchema(connection, physicalTableName);
                if (currentSchema.columns.isEmpty()) {
                    String options = existingTables.stream().sorted().collect(Collectors.joining(", "));
                    throw new IllegalStateException(
                            "Tabela não encontrada ou sem colunas: " + physicalTableName +
                                    ". Tabelas existentes: [" + options + "]"
                    );
                }

                // Lê DDL original, índices e triggers
                String originalCreateTableSql = readCreateTableSql(connection, physicalTableName);
                List<RawIndexDef> originalIndexDefinitions = readIndexCreateSql(connection, physicalTableName);
                List<RawTriggerDef> originalTriggerDefinitions = readTriggerCreateSql(connection, physicalTableName);

                // PK e AUTOINCREMENT
                List<String> primaryKeyColumns = currentSchema.columns.stream()
                        .filter(c -> c.partOfPrimaryKey)
                        .map(c -> c.name)
                        .toList();

                Set<String> autoIncrementColumns = primaryKeyColumns.size() == 1
                        ? detectAutoIncrementColumns(originalCreateTableSql, primaryKeyColumns)
                        : Collections.emptySet();

                // FKs atuais + plano final
                List<ForeignKeySpec> currentForeignKeys = readCurrentForeignKeys(connection, physicalTableName);
                List<ForeignKeySpec> desiredForeignKeys = computeFinalForeignKeys(
                        currentForeignKeys, foreignKeysToAdd, foreignKeysToDrop
                );

                // Normaliza tabelas referenciadas e colunas
                desiredForeignKeys = normalizeReferencedTablesOrFail(desiredForeignKeys, existingTables);
                desiredForeignKeys = normalizeForeignKeyColumnsOrFail(
                        connection, physicalTableName, currentSchema, desiredForeignKeys
                );

                // Cria temporária com FKs finais e copia dados
                String tempTableName   = TEMP_TABLE_PREFIX + physicalTableName;
                String backupTableName = BACKUP_TABLE_PREFIX + physicalTableName;

                String createTempTableSql = buildCreateTableStatement(
                        physicalTableName, currentSchema, desiredForeignKeys, tempTableName, autoIncrementColumns
                );

                createTemporaryTable(connection, createTempTableSql);
                copyDataSameColumns(connection, physicalTableName, tempTableName, currentSchema);

                // Swap: original -> __bak_, temp -> original
                renameTableSafely(connection, physicalTableName, backupTableName);
                renameTableSafely(connection, tempTableName, physicalTableName);

                // Remove backup
                dropTableSafely(connection, backupTableName);

                // Recria índices e triggers
                recreateIndexesFromSqlMaster(connection, physicalTableName, originalIndexDefinitions);
                recreateTriggersFromSqlMaster(connection, physicalTableName, originalTriggerDefinitions);

                // Valida integridade
                enableForeignKeyEnforcement(connection);
                validateReferentialIntegrityOrFail(connection, physicalTableName);

                connection.commit();
                LOGGER.info("Rebuild da tabela '{}' concluído com sucesso.", physicalTableName);

            } catch (Throwable error) {
                safeRollback(connection);
                safelyExecute(connection, PRAGMA_FOREIGN_KEYS_ON);
                throw error;
            } finally {
                safelyExecute(connection, PRAGMA_FOREIGN_KEYS_ON);
                connection.setAutoCommit(previousAutoCommit);
            }
        }
    }

    // =====================================================================================
    // Regras de negócio auxiliares (FKs, normalização, integridade)
    // =====================================================================================

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
     * - parte das atuais
     * - remove as pedidas em drop (match por colunas base OU target referenciado)
     * - adiciona as pedidas em add
     */
    private List<ForeignKeySpec> computeFinalForeignKeys(
            List<ForeignKeySpec> currentForeignKeys,
            List<ForeignKeySpec> foreignKeysToAdd,
            List<ForeignKeySpec> foreignKeysToDrop) {

        List<ForeignKeySpec> finalSet = new ArrayList<>(
                Optional.ofNullable(currentForeignKeys).orElseGet(List::of)
        );

        List<ForeignKeySpec> drops = Optional.ofNullable(foreignKeysToDrop).orElseGet(List::of);
        for (ForeignKeySpec toDrop : drops) {
            finalSet.removeIf(existing ->
                    hasSameBaseColumns(existing, toDrop) || hasSameReferencedTarget(existing, toDrop)
            );
        }

        List<ForeignKeySpec> adds = Optional.ofNullable(foreignKeysToAdd).orElseGet(List::of);
        finalSet.addAll(adds);

        return finalSet;
    }

    private boolean hasSameBaseColumns(ForeignKeySpec a, ForeignKeySpec b) {
        return normalizeCsv(a.baseColumnsCsv).equalsIgnoreCase(normalizeCsv(b.baseColumnsCsv));
    }

    private boolean hasSameReferencedTarget(ForeignKeySpec a, ForeignKeySpec b) {
        return a.referencedTable.equalsIgnoreCase(b.referencedTable)
                && normalizeCsv(a.referencedColumnsCsv).equalsIgnoreCase(normalizeCsv(b.referencedColumnsCsv));
    }

    private static String normalizeCsv(String csv) {
        if (csv == null) return "";
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining(","));
    }

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

    /** Resolve o nome físico real da tabela ou falha com opções disponíveis. */
    private String resolveExistingTableNameOrFail(String requestedName, Set<String> existingTables) {
        if (requestedName == null || requestedName.isBlank()) {
            throw new IllegalArgumentException("Nome da tabela não pode ser nulo/vazio.");
        }

        // 1) igual
        if (existingTables.contains(requestedName)) return requestedName;

        // 2) case-insensitive
        Map<String, String> byLower = existingTables.stream()
                .collect(Collectors.toMap(s -> s.toLowerCase(Locale.ROOT), s -> s, (a, b) -> a, LinkedHashMap::new));
        String lowerMatch = byLower.get(requestedName.toLowerCase(Locale.ROOT));
        if (lowerMatch != null) return lowerMatch;

        // 3) canônico
        Map<String, String> byCanonical = new LinkedHashMap<>();
        for (String t : existingTables) byCanonical.put(canonical(t), t);
        String canonicalMatch = byCanonical.get(canonical(requestedName));
        if (canonicalMatch != null) return canonicalMatch;

        // 4) camel -> snake
        String snakeGuess = camelToUnderscore(requestedName);
        String snakeMatch = byLower.get(snakeGuess.toLowerCase(Locale.ROOT));
        if (snakeMatch != null) return snakeMatch;

        String options = existingTables.stream().sorted().collect(Collectors.joining(", "));
        throw new IllegalStateException(
                "Tabela não encontrada: '" + requestedName + "'. Tabelas existentes: [" + options + "]"
        );
    }

    /** Normaliza o NOME da tabela referenciada em cada FK; falha se não existir. */
    private List<ForeignKeySpec> normalizeReferencedTablesOrFail(
            List<ForeignKeySpec> desiredForeignKeys, Set<String> existingTables) {

        if (desiredForeignKeys == null || desiredForeignKeys.isEmpty()) return desiredForeignKeys;

        Map<String, String> byLower = existingTables.stream()
                .collect(Collectors.toMap(s -> s.toLowerCase(Locale.ROOT), s -> s, (a, b) -> a, LinkedHashMap::new));

        Map<String, String> byCanonical = new LinkedHashMap<>();
        for (String t : existingTables) byCanonical.put(canonical(t), t);

        List<ForeignKeySpec> normalized = new ArrayList<>(desiredForeignKeys.size());
        for (ForeignKeySpec fk : desiredForeignKeys) {
            String requestedRef = fk.getReferencedTable();
            if (requestedRef == null || requestedRef.isBlank()) {
                normalized.add(fk);
                continue;
            }

            // 1) exato
            if (existingTables.contains(requestedRef)) {
                normalized.add(fk);
                continue;
            }

            // 2) case-insensitive
            String lowerMatch = byLower.get(requestedRef.toLowerCase(Locale.ROOT));
            if (lowerMatch != null) {
                if (!lowerMatch.equals(requestedRef)) {
                    LOGGER.info("Normalizando tabela referenciada '{}' -> '{}'", requestedRef, lowerMatch);
                }
                normalized.add(fk.withReferencedTable(lowerMatch));
                continue;
            }

            // 3) canônico
            String canonicalMatch = byCanonical.get(canonical(requestedRef));
            if (canonicalMatch != null) {
                LOGGER.info("Normalizando (canônico) '{}' -> '{}'", requestedRef, canonicalMatch);
                normalized.add(fk.withReferencedTable(canonicalMatch));
                continue;
            }

            // 4) camel -> snake
            String snakeGuess = camelToUnderscore(requestedRef);
            String snakeMatch = byLower.get(snakeGuess.toLowerCase(Locale.ROOT));
            if (snakeMatch != null) {
                LOGGER.info("Normalizando (camel->underscore) '{}' -> '{}'", requestedRef, snakeMatch);
                normalized.add(fk.withReferencedTable(snakeMatch));
                continue;
            }

            String hint = existingTables.stream().sorted().collect(Collectors.joining(", "));
            throw new IllegalStateException(
                    "Tabela referenciada pela FK não existe: '" + requestedRef + "'. " +
                            "Tabelas existentes: [" + hint + "]"
            );
        }
        return normalized;
    }

    /** Normaliza colunas base e referenciadas das FKs contra os nomes físicos. */
    private List<ForeignKeySpec> normalizeForeignKeyColumnsOrFail(
            Connection connection,
            String basePhysicalTableName,
            TableSchema baseTableSchema,
            List<ForeignKeySpec> desiredForeignKeys) throws SQLException {

        Set<String> basePhysicalColumns = baseTableSchema.columns.stream()
                .map(c -> c.name)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Map<String, Set<String>> refTableToColumnsCache = new HashMap<>();

        List<ForeignKeySpec> normalized = new ArrayList<>(desiredForeignKeys.size());
        for (ForeignKeySpec fk : desiredForeignKeys) {
            String normalizedBaseCsv = normalizeColumnsCsvOrFail(
                    fk.getBaseColumnsCsv(),
                    basePhysicalColumns,
                    () -> "FK base (" + basePhysicalTableName + ")"
            );

            String refTable = fk.getReferencedTable();
            Set<String> refPhysicalColumns = refTableToColumnsCache.computeIfAbsent(refTable, t -> {
                try {
                    return readPhysicalColumnNames(connection, t);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });

            String normalizedRefCsv = normalizeColumnsCsvOrFail(
                    fk.getReferencedColumnsCsv(),
                    refPhysicalColumns,
                    () -> "FK referenciada (" + refTable + ")"
            );

            normalized.add(
                    fk.withBaseColumnsCsv(normalizedBaseCsv)
                            .withReferencedColumnsCsv(normalizedRefCsv)
            );
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

    private static String normalizeColumnsCsvOrFail(
            String csv,
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

            // 1) exato
            if (physicalColumns.contains(candidate)) {
                resolved.add(candidate);
                continue;
            }

            // 2) lower
            String lower = byLower.get(candidate.toLowerCase(Locale.ROOT));
            if (lower != null) {
                resolved.add(lower);
                continue;
            }

            // 3) canônico
            String canonical = byCanonical.get(canonical(candidate));
            if (canonical != null) {
                resolved.add(canonical);
                continue;
            }

            // 4) camel -> snake
            String snakeGuess = camelToUnderscore(candidate);
            String snake = byLower.get(snakeGuess.toLowerCase(Locale.ROOT));
            if (snake != null) {
                resolved.add(snake);
                continue;
            }

            String ctx = contextSupplier.get();
            String hint = String.join(", ", physicalColumns);
            throw new IllegalStateException(
                    "Coluna não encontrada " + ctx + ": '" + candidate + "'. Existentes: [" + hint + "]"
            );
        }

        return String.join(",", resolved);
    }

    // =====================================================================================
    // Construção do CREATE TABLE
    // =====================================================================================

    private String buildCreateTableStatement(
            String originalTableName,
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
            String pkList = primaryKeyColumns.stream()
                    .map(SqliteTableRebuilder::quoteIdentifier)
                    .collect(Collectors.joining(", "));
            definitionParts.add("  PRIMARY KEY (" + pkList + ")");
        }

        for (ForeignKeySpec fk : foreignKeys) {
            if (fk.getReferencedTable() == null || fk.getReferencedTable().isBlank()) continue;
            definitionParts.add(buildForeignKeyDefinition(fk));
        }

        create.append(String.join(",\n", definitionParts)).append("\n)");
        return create.toString();
    }

    private String buildColumnDefinition(
            List<String> primaryKeyColumns,
            TableColumn column,
            Set<String> autoIncrementColumns) {

        StringBuilder definition = new StringBuilder();
        definition.append("  ").append(quoteIdentifier(column.name)).append(" ")
                .append(column.typeDeclaration == null ? "" : column.typeDeclaration);

        boolean isSingleColumnPk = primaryKeyColumns.size() == 1 && column.partOfPrimaryKey;
        if (isSingleColumnPk) {
            definition.append(" PRIMARY KEY");
            boolean isIntegerType = column.typeDeclaration != null
                    && column.typeDeclaration.trim().equalsIgnoreCase("INTEGER");
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
                .map(String::trim)
                .map(SqliteTableRebuilder::quoteIdentifier)
                .collect(Collectors.joining(", "));

        String refList = Arrays.stream(fk.getReferencedColumnsCsv().split(","))
                .map(String::trim)
                .map(SqliteTableRebuilder::quoteIdentifier)
                .collect(Collectors.joining(", "));

        StringBuilder sb = new StringBuilder();
        sb.append("  FOREIGN KEY (").append(baseList).append(")")
                .append(" REFERENCES ").append(quoteIdentifier(fk.getReferencedTable()))
                .append(" (").append(refList).append(")");

        if (hasText(fk.getOnDelete())) sb.append(" ON DELETE ").append(fk.getOnDelete());
        if (hasText(fk.getOnUpdate())) sb.append(" ON UPDATE ").append(fk.getOnUpdate());
        if (hasText(fk.getMatch()) && !"NONE".equalsIgnoreCase(fk.getMatch())) {
            sb.append(" MATCH ").append(fk.getMatch());
        }
        return sb.toString();
    }

    // =====================================================================================
    // Leitura de metadados
    // =====================================================================================

    private TableSchema readCurrentTableSchema(Connection connection, String tableName) throws SQLException {
        TableSchema schema = new TableSchema();
        String sql = String.format(PRAGMA_TABLE_INFO, quoteIdentifier(tableName));
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String name   = rs.getString("name");
                String type   = rs.getString("type");
                boolean notNull    = rs.getInt("notnull") == 1;
                String defaultExpr = rs.getString("dflt_value");
                boolean partOfPk   = rs.getInt("pk") == 1;
                schema.columns.add(new TableColumn(name, type, notNull, defaultExpr, partOfPk));
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

        List<ForeignKeySpec> foreignKeys = new ArrayList<>();
        for (List<ForeignKeyRow> rows : groupedByConstraint.values()) {
            String refTable = rows.get(0).referencedTable;
            String onUpd    = rows.get(0).onUpdateAction;
            String onDel    = rows.get(0).onDeleteAction;
            String match    = rows.get(0).matchAction;
            String baseCsv  = rows.stream().map(r -> r.baseColumn).collect(Collectors.joining(","));
            String refCsv   = rows.stream().map(r -> r.referencedColumn).collect(Collectors.joining(","));
            foreignKeys.add(new ForeignKeySpec(baseCsv, refTable, refCsv, onDel, onUpd, match));
        }
        return foreignKeys;
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

    // =====================================================================================
    // Diagnóstico de integridade
    // =====================================================================================

    private void validateReferentialIntegrityOrFail(Connection connection, String rebuiltTableName) {
        List<ForeignKeyViolation> violations = runForeignKeyCheck(connection);
        if (violations.isEmpty()) return;

        String message = buildForeignKeyViolationMessage(rebuiltTableName, violations, connection);
        LOGGER.error(message);
        throw new IllegalStateException(message);
    }

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

    private String buildForeignKeyViolationMessage(
            String rebuiltTableName,
            List<ForeignKeyViolation> violations,
            Connection connection) {

        String header = "Violação(ões) de integridade referencial após rebuild da tabela '" + rebuiltTableName + "'.";
        String details = violations.stream()
                .map(v -> "table=" + v.violatingTable + ", rowid=" + v.violatingRowId +
                        ", parent=" + v.referencedParentTable + ", fkid=" + v.foreignKeyId)
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

    // =====================================================================================
    // Infra: swap / drop / execução
    // =====================================================================================

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

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
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

    // =====================================================================================
    // AUTOINCREMENT (via sqlite_master)
    // =====================================================================================

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

    private static Set<String> detectAutoIncrementColumns(
            String createTableSql, Collection<String> candidatePkColumns) {

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

    // =====================================================================================
    // Modelos internos (imutáveis quando fizer sentido)
    // =====================================================================================

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
                              String onDeleteAction,
                              String onUpdateAction,
                              String matchAction) {
            this.baseColumnsCsv = baseColumnsCsv;
            this.referencedTable = referencedTable;
            this.referencedColumnsCsv = referencedColumnsCsv;
            this.onDeleteAction = onDeleteAction;
            this.onUpdateAction = onUpdateAction;
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
            if (hasText(onDeleteAction)) sb.append(" ON DELETE ").append(onDeleteAction);
            if (hasText(onUpdateAction)) sb.append(" ON UPDATE ").append(onUpdateAction);
            if (hasText(matchAction) && !"NONE".equalsIgnoreCase(matchAction)) {
                sb.append(" MATCH ").append(matchAction);
            }
            return sb.toString();
        }
    }

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

    // =====================================================================================
    // Utilidades de nome
    // =====================================================================================

    /** Remove caracteres não alfanuméricos e deixa minúsculo (para comparações canônicas). */
    private static String canonical(String name) {
        if (name == null) return "";
        return name.replaceAll("[^A-Za-z0-9]", "").toLowerCase(Locale.ROOT);
    }

    /** camelCase → snake_case simples. */
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
            sb.append(Character.toLowerCase(ch));
        }
        return sb.toString();
    }
}