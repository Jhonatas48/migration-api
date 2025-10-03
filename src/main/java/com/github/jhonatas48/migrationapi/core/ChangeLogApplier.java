package com.github.jhonatas48.migrationapi.core;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.CompositeResourceAccessor;
import liquibase.resource.DirectoryResourceAccessor;
import liquibase.resource.ResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Aplica changelogs com pré-processamento para compatibilidade com SQLite:
 *  - Converte addUniqueConstraint -> createIndex(unique: true)
 *  - Remove modifyDataType (SQLite não suporta) e registra pendências
 *
 * Em seguida, aplica o changelog via API direta do Liquibase, detectando
 * automaticamente se o arquivo está no filesystem (ex.: /tmp) ou no classpath.
 */
public final class ChangeLogApplier {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeLogApplier.class);

    private static final String SQLITE_SUFFIX = "-sqlite.yaml";

    // Padrões para varrer YAML gerado
    private static final Pattern ADD_UNIQUE_CONSTRAINT_START =
            Pattern.compile("^\\s*-\\s*addUniqueConstraint\\s*:\\s*$");
    private static final Pattern MODIFY_DATA_TYPE_START =
            Pattern.compile("^\\s*-\\s*modifyDataType\\s*:\\s*$");
    private static final Pattern YAML_KEY_VALUE =
            Pattern.compile("^\\s*([a-zA-Z_][a-zA-Z0-9_]*)\\s*:\\s*(.+?)\\s*$");

    private final DataSource dataSource;

    public ChangeLogApplier(final DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    /**
     * Ajusta o changelog para SQLite e aplica via Liquibase.
     * @param originalChangeLogPath caminho do changelog original (YAML). Pode ser
     *                              absoluto em disco (ex.: %TEMP%) ou um recurso do classpath.
     */
    public void applyChangeLog(final String originalChangeLogPath) {
        Objects.requireNonNull(originalChangeLogPath, "originalChangeLogPath must not be null");

        try {
            final Path originalPath = Path.of(originalChangeLogPath);
            Path pathParaAplicar;

            // Pré-processa apenas se o arquivo existir no filesystem.
            if (Files.exists(originalPath)) {
                final Path adjustedPath = buildSqliteAdjustedPath(originalPath);
                final SqliteAdjustmentResult result = adjustChangelogForSqlite(originalPath, adjustedPath);

                if (result.modified) {
                    pathParaAplicar = adjustedPath;
                    LOGGER.info("YAML ajustado para SQLite gravado em {}", adjustedPath);
                    logPendingTypeChanges(result.pendingTypeChanges);
                } else {
                    pathParaAplicar = originalPath;
                    LOGGER.info("Changelog já compatível com SQLite. Usando arquivo original: {}", originalPath);
                }
            } else {
                // Caso de classpath: não há pré-processamento em disco aqui;
                // se precisar, o gerador pode ser ajustado para depositar em /generated antes.
                LOGGER.info("Changelog não encontrado no filesystem. Tentando no classpath: {}", originalChangeLogPath);
                pathParaAplicar = Path.of(originalChangeLogPath); // apenas para logging/coerência
            }

            applyWithLiquibase(pathParaAplicar, originalChangeLogPath);

        } catch (IOException e) {
            throw new IllegalStateException("Falha ao ajustar o changelog para SQLite: " + e.getMessage(), e);
        } catch (LiquibaseException | SQLException e) {
            throw new IllegalStateException("Falha ao aplicar o changelog ajustado: " + e.getMessage(), e);
        }
    }

    // ========================================================================
    // Liquibase Runner — resolve classpath x filesystem
    // ========================================================================

    /**
     * Aplica o changelog usando o accessor correto:
     * - Se o arquivo existir no filesystem, usamos DirectoryResourceAccessor apontando para o diretório do arquivo.
     * - Também combinamos com ClassLoaderResourceAccessor (fallback para dependências referenciadas).
     * - Se não existir no filesystem, tentamos apenas via classpath.
     */
    private void applyWithLiquibase(final Path pathParaAplicar, final String logicalOriginalArg)
            throws SQLException, LiquibaseException {

        try (Connection jdbcConnection = dataSource.getConnection()) {
            final Database database = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(new JdbcConnection(jdbcConnection));

            final boolean existsOnFs = Files.exists(pathParaAplicar);
            final ResourceAccessor resourceAccessor;
            final String changelogLogicalPath;

            if (existsOnFs) {
                final Path parent = Optional.ofNullable(pathParaAplicar.getParent()).orElse(Path.of(".")).toAbsolutePath();
                resourceAccessor = new CompositeResourceAccessor(
                        new DirectoryResourceAccessor(parent.toFile()),
                        new ClassLoaderResourceAccessor(Thread.currentThread().getContextClassLoader())
                );
                changelogLogicalPath = pathParaAplicar.getFileName().toString();
                LOGGER.debug("Aplicando changelog pelo filesystem. root={}, file={}", parent, changelogLogicalPath);
            } else {
                // Apenas classpath. Aqui usamos a string original passada pelo chamador,
                // que deve corresponder ao path do recurso no classpath.
                resourceAccessor = new ClassLoaderResourceAccessor(Thread.currentThread().getContextClassLoader());
                changelogLogicalPath = logicalOriginalArg;
                LOGGER.debug("Aplicando changelog pelo classpath. resource={}", changelogLogicalPath);
            }

            final Liquibase liquibase = new Liquibase(changelogLogicalPath, resourceAccessor, database);
            liquibase.update((Contexts) null, (LabelExpression) null);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    // ========================================================================
    // Ajuste do YAML para SQLite
    // ========================================================================

    private SqliteAdjustmentResult adjustChangelogForSqlite(final Path sourceYaml, final Path targetYaml) throws IOException {
        final List<String> adjustedLines = new ArrayList<>();
        final List<PendingTypeChange> pendingTypeChanges = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(sourceYaml, StandardCharsets.UTF_8)) {
            String line;
            boolean insideAddUnique = false;
            boolean insideModifyType = false;

            Map<String, String> addUniqueBlock = new LinkedHashMap<>();
            Map<String, String> modifyTypeBlock = new LinkedHashMap<>();
            int blockIndent = 0;

            while ((line = reader.readLine()) != null) {
                if (ADD_UNIQUE_CONSTRAINT_START.matcher(line).find()) {
                    insideAddUnique = true;
                    addUniqueBlock.clear();
                    blockIndent = leadingSpaces(line);
                    continue;
                }
                if (MODIFY_DATA_TYPE_START.matcher(line).find()) {
                    insideModifyType = true;
                    modifyTypeBlock.clear();
                    blockIndent = leadingSpaces(line);
                    continue;
                }

                if (insideAddUnique) {
                    if (leadingSpaces(line) > blockIndent && YAML_KEY_VALUE.matcher(line).find()) {
                        final Matcher m = YAML_KEY_VALUE.matcher(line);
                        if (m.find()) addUniqueBlock.put(m.group(1).trim(), stripYamlScalar(m.group(2)));
                        continue;
                    } else {
                        adjustedLines.addAll(renderCreateUniqueIndexBlock(blockIndent, addUniqueBlock));
                        insideAddUnique = false;
                        addUniqueBlock.clear();
                        // cai para processamento normal da linha atual
                    }
                }

                if (insideModifyType) {
                    if (leadingSpaces(line) > blockIndent && YAML_KEY_VALUE.matcher(line).find()) {
                        final Matcher m = YAML_KEY_VALUE.matcher(line);
                        if (m.find()) modifyTypeBlock.put(m.group(1).trim(), stripYamlScalar(m.group(2)));
                        continue;
                    } else {
                        final PendingTypeChange pending = toPendingTypeChange(modifyTypeBlock);
                        if (pending != null) pendingTypeChanges.add(pending);
                        insideModifyType = false;
                        modifyTypeBlock.clear();
                        // cai para processamento normal da linha atual
                    }
                }

                adjustedLines.add(line);
            }

            if (insideAddUnique) {
                adjustedLines.addAll(renderCreateUniqueIndexBlock(blockIndent, addUniqueBlock));
            }
            if (insideModifyType) {
                final PendingTypeChange pending = toPendingTypeChange(modifyTypeBlock);
                if (pending != null) pendingTypeChanges.add(pending);
            }
        }

        final boolean modified = !pendingTypeChanges.isEmpty() || fileContains(sourceYaml, ADD_UNIQUE_CONSTRAINT_START);

        if (modified) {
            if (targetYaml.getParent() != null && !Files.exists(targetYaml.getParent())) {
                Files.createDirectories(targetYaml.getParent());
            }
            try (BufferedWriter writer = Files.newBufferedWriter(targetYaml, StandardCharsets.UTF_8)) {
                for (String l : adjustedLines) {
                    writer.write(l);
                    writer.newLine();
                }
            }
        }

        return new SqliteAdjustmentResult(modified, pendingTypeChanges);
    }

    private static boolean fileContains(final Path file, final Pattern pattern) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (pattern.matcher(line).find()) return true;
            }
        }
        return false;
    }

    private static int leadingSpaces(final String line) {
        int i = 0;
        while (i < line.length() && line.charAt(i) == ' ') i++;
        return i;
    }

    private static String stripYamlScalar(final String raw) {
        if (raw == null) return "";
        final String value = raw.trim();
        if ((value.startsWith("'") && value.endsWith("'")) || (value.startsWith("\"") && value.endsWith("\""))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private static List<String> renderCreateUniqueIndexBlock(final int indent, final Map<String, String> uniqueBlock) {
        final String tableName = uniqueBlock.getOrDefault("tableName", "");
        final String columnNamesCsv = uniqueBlock.getOrDefault("columnNames", "");
        final String indexName = uniqueBlock.getOrDefault("constraintName",
                generateUniqueIndexName(tableName, columnNamesCsv));

        final List<String> columns = splitCsv(columnNamesCsv);
        final String pad = " ".repeat(indent);

        final List<String> lines = new ArrayList<>();
        lines.add(pad + "- createIndex:");
        lines.add(pad + "    tableName: " + quoteYaml(tableName));
        lines.add(pad + "    indexName: " + quoteYaml(indexName));
        lines.add(pad + "    unique: true");
        lines.add(pad + "    columns:");
        for (String col : columns) {
            lines.add(pad + "      - column:");
            lines.add(pad + "          name: " + quoteYaml(col));
        }
        return lines;
    }

    private static String generateUniqueIndexName(final String tableName, final String columnNamesCsv) {
        final String base = (tableName + "_" + columnNamesCsv.replace(",", "_") + "_uq")
                .replaceAll("[^A-Za-z0-9_]", "_");
        return base.length() > 60 ? base.substring(0, 60) : base;
    }

    private static List<String> splitCsv(final String csv) {
        if (csv == null || csv.isBlank()) return List.of();
        final String[] parts = csv.split(",");
        final List<String> out = new ArrayList<>(parts.length);
        for (String p : parts) {
            final String v = p.trim();
            if (!v.isEmpty()) out.add(v);
        }
        return out;
    }

    private static String quoteYaml(final String value) {
        if (value == null) return "''";
        return "'" + value.replace("'", "''") + "'";
    }

    private static PendingTypeChange toPendingTypeChange(final Map<String, String> block) {
        final String table = block.get("tableName");
        final String column = block.get("columnName");
        final String newType = block.get("newDataType");
        if (isBlank(table) || isBlank(column) || isBlank(newType)) return null;
        return new PendingTypeChange(table, column, newType);
    }

    private static boolean isBlank(final String s) {
        return s == null || s.trim().isEmpty();
    }

    private static Path buildSqliteAdjustedPath(final Path originalPath) {
        final String name = originalPath.getFileName().toString();
        final String adjusted = name.endsWith(".yaml")
                ? name.substring(0, name.length() - 5) + SQLITE_SUFFIX
                : name + SQLITE_SUFFIX;
        return originalPath.getParent() == null
                ? Path.of(adjusted)
                : originalPath.getParent().resolve(adjusted);
    }

    private void logPendingTypeChanges(final List<PendingTypeChange> pendingTypeChanges) {
        if (pendingTypeChanges.isEmpty()) return;
        LOGGER.warn("modifyDataType detectado e removido (SQLite não suporta). Pendências:");
        for (PendingTypeChange p : pendingTypeChanges) {
            LOGGER.warn(" - table='{}', column='{}', newType='{}'", p.tableName, p.columnName, p.newDataType);
        }
        LOGGER.warn("Sugestão: trate mudanças de tipo via rebuild de tabela (ex.: SqliteTableRebuilder).");
    }

    // ========================================================================
    // Tipos auxiliares (imutáveis)
    // ========================================================================

    private static final class SqliteAdjustmentResult {
        final boolean modified;
        final List<PendingTypeChange> pendingTypeChanges;

        SqliteAdjustmentResult(final boolean modified, final List<PendingTypeChange> pendingTypeChanges) {
            this.modified = modified;
            this.pendingTypeChanges = List.copyOf(pendingTypeChanges);
        }
    }

    private static final class PendingTypeChange {
        final String tableName;
        final String columnName;
        final String newDataType;

        PendingTypeChange(final String tableName, final String columnName, final String newDataType) {
            this.tableName = tableName;
            this.columnName = columnName;
            this.newDataType = newDataType;
        }
    }
}