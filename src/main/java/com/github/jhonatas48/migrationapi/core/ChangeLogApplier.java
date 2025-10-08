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
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Aplica um changelog YAML ao SQLite, garantindo pré-processamento de compatibilidade.
 * - Se o YAML estiver no filesystem: ajusta (se necessário) e aplica.
 * - Se o YAML estiver no classpath: carrega, ajusta e materializa um arquivo *ajustado* em target/,
 *   então aplica a partir desse arquivo.
 */
public final class ChangeLogApplier {

    private static final Logger LOG = LoggerFactory.getLogger(ChangeLogApplier.class);

    private static final String GENERATED_DIR = "target/generated-changelogs";
    private static final String SQLITE_SUFFIX = "-sqlite.yaml";

    private final DataSource dataSource;

    public ChangeLogApplier(final DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    /** Aplica o changelog ao banco. Aceita caminho de filesystem ou classpath. */
    public void applyChangeLog(final String originalPathOrClasspath) {
        Objects.requireNonNull(originalPathOrClasspath, "originalPathOrClasspath must not be null");
        try (Connection jdbc = dataSource.getConnection()) {
            final Database liquibaseDb = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(new JdbcConnection(jdbc));

            final ChangelogMaterialization mat = materializeAdjustedChangelog(originalPathOrClasspath);
            final ResourceAccessor accessor = buildAccessor(mat.baseDirectory);

            LOG.info("Aplicando changelog ajustado: {}", mat.logicalFileName);
            final Liquibase lb = new Liquibase(mat.logicalFileName, accessor, liquibaseDb);
            lb.update((Contexts) null, (LabelExpression) null);

        } catch (SQLException | LiquibaseException e) {
            throw new IllegalStateException("Falha ao aplicar o changelog ajustado: " + e.getMessage(), e);
        }
    }

    // ===== Materialização do YAML ajustado =====

    private ChangelogMaterialization materializeAdjustedChangelog(final String input) {
        final Path fsPath = Path.of(input);
        if (Files.exists(fsPath)) {
            final List<String> original = readAllLines(fsPath);
            final SqliteChangeLogAdjuster.AdjustResult result = SqliteChangeLogAdjuster.adjustToSqlite(original);
            final Path outDir = fsPath.getParent() != null ? fsPath.getParent() : Path.of(".");
            final Path outFile = outDir.resolve(sqliteName(fsPath.getFileName().toString()));
            final Path adjustedPath = SqliteChangeLogAdjuster.writeAdjustedFile(outFile, result.adjustedLines);

            logAdjustSummary(result);
            return new ChangelogMaterialization(outDir.toAbsolutePath(), adjustedPath.getFileName().toString());
        }

        final Optional<List<String>> inClasspath = readClasspathLines(input);
        final List<String> originalLines = inClasspath.orElseThrow(() ->
                new IllegalStateException("Recurso não encontrado no classpath: " + input + ". Coloque em src/test/resources."));
        final SqliteChangeLogAdjuster.AdjustResult result = SqliteChangeLogAdjuster.adjustToSqlite(originalLines);

        final Path outDir = Path.of(GENERATED_DIR);
        final String adjustedName = sqliteName(Path.of(input).getFileName().toString());
        final Path outFile = outDir.resolve(adjustedName);
        final Path adjustedPath = SqliteChangeLogAdjuster.writeAdjustedFile(outFile, result.adjustedLines);

        logAdjustSummary(result);
        return new ChangelogMaterialization(outDir.toAbsolutePath(), adjustedPath.getFileName().toString());
    }

    private static void logAdjustSummary(SqliteChangeLogAdjuster.AdjustResult result) {
        if (!result.modified) {
            LOG.info("Changelog já compatível com SQLite (nenhum ajuste necessário).");
            return;
        }
        LOG.info("Changelog ajustado para SQLite.");
        if (!result.removedForeignKeyBlocks.isEmpty()) {
            LOG.warn("Blocos FK removidos: {}", result.removedForeignKeyBlocks.size());
        }
        if (!result.pendingTypeChanges.isEmpty()) {
            LOG.warn("modifyDataType removidos (pendências={}):", result.pendingTypeChanges.size());
            result.pendingTypeChanges.forEach(p ->
                    LOG.warn(" - table='{}', column='{}', newType='{}'", p.tableName, p.columnName, p.newDataType));
        }
    }

    private static List<String> readAllLines(Path path) {
        try {
            return Files.readAllLines(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Falha ao ler " + path, e);
        }
    }

    private static Optional<List<String>> readClasspathLines(String resourcePath) {
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try (var in = cl.getResourceAsStream(resourcePath)) {
            if (in == null) return Optional.empty();
            try (var br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                return Optional.of(br.lines().toList());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Falha ao ler do classpath: " + resourcePath, e);
        }
    }

    private static ResourceAccessor buildAccessor(Path baseDir) {
        try {
            return new CompositeResourceAccessor(
                    new DirectoryResourceAccessor(baseDir.toFile()),
                    new ClassLoaderResourceAccessor(Thread.currentThread().getContextClassLoader())
            );
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static String sqliteName(String originalFileName) {
        return originalFileName.endsWith(".yaml")
                ? originalFileName.replace(".yaml", SQLITE_SUFFIX)
                : originalFileName + SQLITE_SUFFIX;
    }

    // ===== DTO interno =====
    private record ChangelogMaterialization(Path baseDirectory, String logicalFileName) { }

    /**
     * Torna um changelog Liquibase mais compatível com SQLite.
     * - remove addForeignKeyConstraint/dropForeignKeyConstraint
     * - remove modifyDataType (e loga pendências)
     * - converte addUniqueConstraint -> createIndex(unique: true)
     * - injeta preConditions quando o changeset altera tabela potencialmente inexistente
     */
    static final class SqliteChangeLogAdjuster {

        private static final Pattern START_CHANGESET       = Pattern.compile("^\\s*-\\s*changeSet\\s*:\\s*$");
        private static final Pattern START_ADD_UNIQUE      = Pattern.compile("^\\s*-\\s*addUniqueConstraint\\s*:\\s*$");
        private static final Pattern START_MODIFY_TYPE     = Pattern.compile("^\\s*-\\s*modifyDataType\\s*:\\s*$");
        private static final Pattern START_ADD_FK          = Pattern.compile("^\\s*-\\s*addForeignKeyConstraint\\s*:\\s*$");
        private static final Pattern START_DROP_FK         = Pattern.compile("^\\s*-\\s*dropForeignKeyConstraint\\s*:\\s*$");
        private static final Pattern YAML_KEY_VALUE        = Pattern.compile("^\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*:\\s*(.*?)\\s*$");
        private static final Pattern START_GENERIC_CHANGE  = Pattern.compile("^\\s*-\\s*([A-Za-z][A-Za-z0-9_]*)\\s*:\\s*$");

        // ⚠️ não pule mais createIndex — precisamos de preconditions pra evitar erro no SQLite
        private static final Set<String> CHANGE_TYPES_SKIP_PRECOND = Set.of(
                "createTable", "addUniqueConstraint"
        );

        private static final List<String> TABLE_NAME_KEYS = List.of(
                "tableName", "baseTableName", "oldTableName"
        );

        static AdjustResult adjustToSqlite(final List<String> originalLines) {
            Objects.requireNonNull(originalLines, "originalLines must not be null");

            final List<String> output = new ArrayList<>(originalLines.size());
            final List<PendingTypeChange> pendingTypeChanges = new ArrayList<>();
            final List<RemovedForeignKeyBlock> removedForeignKeyBlocks = new ArrayList<>();
            boolean modified = false;

            for (int i = 0; i < originalLines.size(); i++) {
                final String line = originalLines.get(i);

                if (isStart(line, START_CHANGESET)) {
                    final int baseIndent = indentOf(line);
                    final int endIdx = findBlockEnd(originalLines, baseIndent, i);
                    final List<String> csBlock = originalLines.subList(i, endIdx + 1);

                    final List<String> adjustedChangeSet =
                            adjustSingleChangeSetBlock(csBlock, pendingTypeChanges, removedForeignKeyBlocks);

                    output.addAll(adjustedChangeSet);
                    modified = true;
                    i = endIdx;
                    continue;
                }

                if (isStart(line, START_ADD_FK) || isStart(line, START_DROP_FK)) {
                    final boolean isAdd = isStart(line, START_ADD_FK);
                    final int baseIndent = indentOf(line);
                    final Map<String, String> fields = readBlockFields(originalLines, baseIndent, i + 1);

                    removedForeignKeyBlocks.add(isAdd
                            ? RemovedForeignKeyBlock.add(fields)
                            : RemovedForeignKeyBlock.drop(fields));

                    modified = true;
                    i = skipBlock(originalLines, baseIndent, i);
                    continue;
                }

                if (isStart(line, START_ADD_UNIQUE)) {
                    final int baseIndent = indentOf(line);
                    final Map<String, String> fields = readBlockFields(originalLines, baseIndent, i + 1);

                    output.addAll(renderCreateUniqueIndexBlock(baseIndent, fields));
                    modified = true;
                    i = skipBlock(originalLines, baseIndent, i);
                    continue;
                }

                if (isStart(line, START_MODIFY_TYPE)) {
                    final int baseIndent = indentOf(line);
                    final Map<String, String> fields = readBlockFields(originalLines, baseIndent, i + 1);

                    toPendingTypeChange(fields).ifPresent(pendingTypeChanges::add);
                    modified = true;
                    i = skipBlock(originalLines, baseIndent, i);
                    continue;
                }

                output.add(line);
            }

            return new AdjustResult(modified, output, pendingTypeChanges, removedForeignKeyBlocks);
        }

        private static List<String> adjustSingleChangeSetBlock(
                List<String> changeSetLines,
                List<PendingTypeChange> pendingTypeChanges,
                List<RemovedForeignKeyBlock> removedForeignKeyBlocks
        ) {
            final int baseIndent = indentOf(changeSetLines.get(0));
            final List<String> rewritten = new ArrayList<>(changeSetLines.size());

            // "- changeSet:"
            rewritten.add(changeSetLines.get(0));

            // Metadados (id, author, labels etc.)
            int cursor = 1;
            while (cursor < changeSetLines.size() && isMetadataLine(changeSetLines.get(cursor), baseIndent)) {
                rewritten.add(changeSetLines.get(cursor));
                cursor++;
            }

            // Localiza "changes:"
            final int changesIdx = findChangesLineIndex(changeSetLines, baseIndent, cursor);
            final boolean hasChangesSection = changesIdx >= 0;

            if (!hasChangesSection) {
                while (cursor < changeSetLines.size()) {
                    rewritten.add(changeSetLines.get(cursor++));
                }
                return rewritten;
            }

            // Copia tudo até "changes:"
            while (cursor < changesIdx) rewritten.add(changeSetLines.get(cursor++));

            final int changesIndent = indentOf(changeSetLines.get(changesIdx));
            final List<List<String>> changeBlocks = readChangeBlocks(changeSetLines, changesIdx, changesIndent);

            final ChangeSetAnalysis analysis = analyzeChangeBlocks(changeBlocks, pendingTypeChanges, removedForeignKeyBlocks);

            // Injeta preconditions **irmão de "changes:"** se fizer sentido
            if (analysis.shouldInjectPreconditions() && analysis.tableForPrecondition != null && !analysis.tableForPrecondition.isBlank()) {
                rewritten.addAll(renderPreconditionsBlock(changesIndent, analysis.tableForPrecondition));
            }

            // Agora "changes:" e os blocos reescritos
            rewritten.add(changeSetLines.get(changesIdx)); // "changes:"
            rewritten.addAll(analysis.rewrittenBlocks);

            return rewritten;
        }

        private static boolean isMetadataLine(String line, int changeSetBaseIndent) {
            final int lineIndent = indentOf(line);
            if (lineIndent <= changeSetBaseIndent) return false;
            return !line.trim().startsWith("changes:");
        }

        private static int findChangesLineIndex(List<String> lines, int baseIndent, int start) {
            for (int i = start; i < lines.size(); i++) {
                final String trimmed = lines.get(i).trim();
                if (indentOf(lines.get(i)) > baseIndent && "changes:".equals(trimmed)) {
                    return i;
                }
            }
            return -1;
        }

        private static List<List<String>> readChangeBlocks(List<String> lines, int changesIdx, int changesIndent) {
            final List<List<String>> blocks = new ArrayList<>();
            int i = changesIdx + 1;
            while (i < lines.size()) {
                final String current = lines.get(i);
                final int ind = indentOf(current);
                if (ind <= changesIndent) break;

                if (isStart(current, START_GENERIC_CHANGE)) {
                    final int base = indentOf(current);
                    final int end = findBlockEnd(lines, base, i);
                    blocks.add(new ArrayList<>(lines.subList(i, end + 1)));
                    i = end + 1;
                    continue;
                }

                blocks.add(List.of(current));
                i++;
            }
            return blocks;
        }

        private static ChangeSetAnalysis analyzeChangeBlocks(
                List<List<String>> rawBlocks,
                List<PendingTypeChange> pendingTypeChanges,
                List<RemovedForeignKeyBlock> removedForeignKeyBlocks
        ) {
            final List<String> rewritten = new ArrayList<>();
            final Set<String> tablesMentionedForPrecond = new LinkedHashSet<>();
            boolean hasCreateTableForAny = false;

            for (List<String> block : rawBlocks) {
                if (block.isEmpty()) continue;

                final String first = block.get(0);
                final int baseIndent = indentOf(first);

                if (isStart(first, START_ADD_FK) || isStart(first, START_DROP_FK)) {
                    final boolean isAdd = isStart(first, START_ADD_FK);
                    final Map<String, String> fields = readBlockFields(block, baseIndent, 1);
                    removedForeignKeyBlocks.add(isAdd
                            ? RemovedForeignKeyBlock.add(fields)
                            : RemovedForeignKeyBlock.drop(fields));
                    continue;
                }

                if (isStart(first, START_ADD_UNIQUE)) {
                    final Map<String, String> fields = readBlockFields(block, baseIndent, 1);
                    rewritten.addAll(renderCreateUniqueIndexBlock(baseIndent, fields));
                    continue;
                }

                if (isStart(first, START_MODIFY_TYPE)) {
                    final Map<String, String> fields = readBlockFields(block, baseIndent, 1);
                    toPendingTypeChange(fields).ifPresent(pendingTypeChanges::add);
                    continue;
                }

                // mantém bloco original
                rewritten.addAll(block);

                final String changeType = extractChangeType(first).orElse("");

                if ("createTable".equals(changeType)) {
                    hasCreateTableForAny = true;
                }

                // coletar possíveis nomes de tabela para o precondition
                if (!CHANGE_TYPES_SKIP_PRECOND.contains(changeType)) {
                    final Map<String, String> fields = readBlockFields(block, baseIndent, 1);
                    resolveTableNameForPrecond(fields).ifPresent(tablesMentionedForPrecond::add);
                }
            }

            final String tableForPrecondition = pickSingleTable(tablesMentionedForPrecond);
            final boolean shouldInjectPreconditions =
                    tableForPrecondition != null && !hasCreateTableForAny;

            return new ChangeSetAnalysis(rewritten, shouldInjectPreconditions, tableForPrecondition);
        }

        private static Optional<String> resolveTableNameForPrecond(Map<String, String> fields) {
            for (String key : TABLE_NAME_KEYS) {
                final String v = fields.get(key);
                if (!isBlank(v)) return Optional.of(v);
            }
            return Optional.empty();
        }

        private static String pickSingleTable(Set<String> tables) {
            if (tables.isEmpty()) return null;
            if (tables.size() == 1) return tables.iterator().next();
            return null; // mais de uma tabela: não injeta para evitar "null"
        }

        private static Optional<String> extractChangeType(String firstLine) {
            final Matcher m = START_GENERIC_CHANGE.matcher(firstLine);
            if (!m.find()) return Optional.empty();
            return Optional.ofNullable(m.group(1));
        }

        /** Renderiza preConditions como irmão de "changes:" */
        private static List<String> renderPreconditionsBlock(int indent, String tableName) {
            final String pad  = " ".repeat(indent);       // preConditions:
            final String pad2 = " ".repeat(indent + 2);   // onFail/onError/and:
            final String pad4 = " ".repeat(indent + 4);   // - tableExists:
            final String pad6 = " ".repeat(indent + 6);   // tableName:

            List<String> out = new ArrayList<>();
            out.add(pad  + "preConditions:");
            out.add(pad2 + "onFail: MARK_RAN");
            out.add(pad2 + "onError: MARK_RAN");
            out.add(pad2 + "and:");
            out.add(pad4 + "- tableExists:");
            out.add(pad6 + "tableName: '" + tableName + "'");
            return out;
        }

        // ---------- Escrita segura do arquivo ajustado ----------
        static Path writeAdjustedFile(final Path targetFile, final List<String> adjustedLines) {
            Objects.requireNonNull(targetFile, "targetFile must not be null");
            Objects.requireNonNull(adjustedLines, "adjustedLines must not be null");

            try {
                final Path parent = targetFile.getParent();
                if (parent != null) Files.createDirectories(parent);

                try (BufferedWriter writer = Files.newBufferedWriter(targetFile, StandardCharsets.UTF_8)) {
                    for (String line : adjustedLines) {
                        writer.write(line);
                        writer.newLine();
                    }
                }
                return targetFile;
            } catch (IOException e) {
                throw new UncheckedIOException("Falha ao gravar YAML ajustado em " + targetFile, e);
            }
        }

        // ---------- Utilidades de parsing ----------
        private static boolean isStart(String line, Pattern p) {
            return p.matcher(line).find();
        }

        private static int indentOf(String s) {
            int i = 0;
            while (i < s.length() && s.charAt(i) == ' ') i++;
            return i;
        }

        private static String stripQuotes(String raw) {
            if (raw == null) return "";
            final String t = raw.trim();
            if ((t.startsWith("'") && t.endsWith("'")) || (t.startsWith("\"") && t.endsWith("\""))) {
                return t.substring(1, t.length() - 1);
            }
            return t;
        }

        private static Map<String, String> readBlockFields(List<String> lines, int baseIndent, int startIdx) {
            final Map<String, String> fields = new LinkedHashMap<>();
            int j = startIdx;
            while (j < lines.size() && indentOf(lines.get(j)) > baseIndent) {
                final Matcher kv = YAML_KEY_VALUE.matcher(lines.get(j));
                if (kv.find()) {
                    fields.put(kv.group(1).trim(), stripQuotes(kv.group(2)));
                }
                j++;
            }
            return fields;
        }

        private static int skipBlock(List<String> lines, int baseIndent, int currentIdx) {
            int j = currentIdx + 1;
            while (j < lines.size() && indentOf(lines.get(j)) > baseIndent) j++;
            return j - 1;
        }

        private static int findBlockEnd(List<String> lines, int baseIndent, int startIdx) {
            int j = startIdx + 1;
            while (j < lines.size() && indentOf(lines.get(j)) > baseIndent) j++;
            return j - 1;
        }

        private static List<String> renderCreateUniqueIndexBlock(int baseIndent, Map<String, String> uniqueFields) {
            final String pad = " ".repeat(baseIndent);
            final String table = uniqueFields.getOrDefault("tableName", "");
            final String csvCols = uniqueFields.getOrDefault("columnNames", "");
            final String indexName = uniqueFields.getOrDefault("constraintName",
                    buildUniqueIndexName(table, csvCols));

            final List<String> cols = new ArrayList<>();
            for (String p : csvCols.split(",")) {
                final String c = p.trim();
                if (!c.isEmpty()) cols.add(c);
            }

            final List<String> out = new ArrayList<>();
            out.add(pad + "- createIndex:");
            out.add(pad + "    tableName: '" + table + "'");
            out.add(pad + "    indexName: '" + indexName + "'");
            out.add(pad + "    unique: true");
            out.add(pad + "    columns:");
            for (String c : cols) {
                out.add(pad + "      - column:");
                out.add(pad + "          name: '" + c + "'");
            }
            return out;
        }

        private static String buildUniqueIndexName(String table, String csvCols) {
            final String base = (table + "_" + csvCols.replace(",", "_") + "_uq")
                    .replaceAll("[^A-Za-z0-9_]", "_");
            return base.length() > 60 ? base.substring(0, 60) : base;
        }

        private static Optional<PendingTypeChange> toPendingTypeChange(Map<String, String> block) {
            final String table = block.get("tableName");
            final String column = block.get("columnName");
            final String newType = block.get("newDataType");
            if (isBlank(table) || isBlank(column) || isBlank(newType)) return Optional.empty();
            return Optional.of(new PendingTypeChange(table, column, newType));
        }

        private static boolean isBlank(String s) {
            return s == null || s.trim().isEmpty();
        }

        // ===== Result types =====
        private record ChangeSetAnalysis(List<String> rewrittenBlocks,
                                         boolean injectPreconditions,
                                         String tableForPrecondition) {
            boolean shouldInjectPreconditions() { return injectPreconditions; }
        }

        static final class AdjustResult {
            final boolean modified;
            final List<String> adjustedLines;
            final List<PendingTypeChange> pendingTypeChanges;
            final List<RemovedForeignKeyBlock> removedForeignKeyBlocks;

            AdjustResult(boolean modified,
                         List<String> adjustedLines,
                         List<PendingTypeChange> pendingTypeChanges,
                         List<RemovedForeignKeyBlock> removedForeignKeyBlocks) {
                this.modified = modified;
                this.adjustedLines = List.copyOf(adjustedLines);
                this.pendingTypeChanges = List.copyOf(pendingTypeChanges);
                this.removedForeignKeyBlocks = List.copyOf(removedForeignKeyBlocks);
            }
        }

        static final class PendingTypeChange {
            final String tableName;
            final String columnName;
            final String newDataType;
            PendingTypeChange(String tableName, String columnName, String newDataType) {
                this.tableName = tableName;
                this.columnName = columnName;
                this.newDataType = newDataType;
            }
        }

        static final class RemovedForeignKeyBlock {
            final String operation; // addForeignKeyConstraint | dropForeignKeyConstraint
            final String tableName;
            final String referencedTableName;
            final String constraintName;

            private RemovedForeignKeyBlock(String operation, String tableName, String referencedTableName, String constraintName) {
                this.operation = operation;
                this.tableName = tableName;
                this.referencedTableName = referencedTableName;
                this.constraintName = constraintName;
            }
            static RemovedForeignKeyBlock add(Map<String, String> fields) {
                return new RemovedForeignKeyBlock(
                        "addForeignKeyConstraint",
                        fields.get("baseTableName"),
                        fields.get("referencedTableName"),
                        fields.get("constraintName"));
            }
            static RemovedForeignKeyBlock drop(Map<String, String> fields) {
                return new RemovedForeignKeyBlock(
                        "dropForeignKeyConstraint",
                        fields.get("baseTableName"),
                        fields.get("referencedTableName"),
                        fields.get("constraintName"));
            }
        }
    }
}
