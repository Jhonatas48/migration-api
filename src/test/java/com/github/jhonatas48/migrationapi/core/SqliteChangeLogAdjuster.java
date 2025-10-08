package com.github.jhonatas48.migrationapi.core;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Responsável por tornar um changelog Liquibase compatível com SQLite.
 * Regras:
 *  - remove addForeignKeyConstraint / dropForeignKeyConstraint (não suportados)
 *  - remove modifyDataType (não suportado) e registra pendência (log externo)
 *  - converte addUniqueConstraint -> createIndex(unique: true)
 *
 * Não utiliza parser YAML, apenas transformação de texto por blocos, garantindo
 * previsibilidade p/ cases gerados e evitando falhas de indentação.
 */
public class SqliteChangeLogAdjuster {

    private static final Pattern START_ADD_UNIQUE = Pattern.compile("^\\s*-\\s*addUniqueConstraint\\s*:\\s*$");
    private static final Pattern START_MODIFY_TYPE = Pattern.compile("^\\s*-\\s*modifyDataType\\s*:\\s*$");
    private static final Pattern START_ADD_FK     = Pattern.compile("^\\s*-\\s*addForeignKeyConstraint\\s*:\\s*$");
    private static final Pattern START_DROP_FK    = Pattern.compile("^\\s*-\\s*dropForeignKeyConstraint\\s*:\\s*$");
    private static final Pattern YAML_KEY_VALUE   = Pattern.compile("^\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*:\\s*(.*?)\\s*$");

    static AdjustResult adjustToSqlite(final List<String> originalLines) {
        Objects.requireNonNull(originalLines, "originalLines must not be null");

        final List<String> output = new ArrayList<>(originalLines.size());
        final List<PendingTypeChange> pendingTypeChanges = new ArrayList<>();
        final List<RemovedForeignKeyBlock> removedForeignKeyBlocks = new ArrayList<>();
        boolean modified = false;

        for (int i = 0; i < originalLines.size(); i++) {
            final String line = originalLines.get(i);

            if (isStart(line, START_ADD_FK) || isStart(line, START_DROP_FK)) {
                final boolean isAdd = isStart(line, START_ADD_FK);
                final int baseIndent = indentOf(line);
                final Map<String, String> fields = readBlockFields(originalLines, baseIndent, i + 1);

                removedForeignKeyBlocks.add(isAdd
                        ? RemovedForeignKeyBlock.add(fields)
                        : RemovedForeignKeyBlock.drop(fields));

                modified = true;
                i = skipBlock(originalLines, baseIndent, i); // remove todo o bloco
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

    static Path writeAdjustedFile(final Path targetFile, final List<String> adjustedLines) {
        try {
            if (targetFile.getParent() != null) Files.createDirectories(targetFile.getParent());
            try (BufferedWriter writer = Files.newBufferedWriter(targetFile, StandardCharsets.UTF_8)) {
                for (String l : adjustedLines) {
                    writer.write(l);
                    writer.newLine();
                }
            }
            return targetFile;
        } catch (IOException e) {
            throw new UncheckedIOException("Falha ao gravar YAML ajustado em " + targetFile, e);
        }
    }

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
