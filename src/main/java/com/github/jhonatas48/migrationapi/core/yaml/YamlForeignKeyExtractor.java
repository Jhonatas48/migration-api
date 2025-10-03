package com.github.jhonatas48.migrationapi.core.yaml;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extrai operações de FK do YAML (add/drop) e retorna um YAML "limpo" para o SQLite,
 * sem esses changes, além de um mapa com o que precisa ser recriado via "table rebuild".
 *
 * Parsing simples por regex/linhas (sem SnakeYAML para reduzir dependência).
 * Entende campos comuns: baseTableName, baseColumnNames, referencedTableName,
 * referencedColumnNames, onDelete, onUpdate e constraintName (quando houver).
 */
public class YamlForeignKeyExtractor {

    public static final class ForeignKeyOperation {
        public enum Kind { ADD, DROP }

        private final Kind kind;
        private final String baseTable;
        private final String baseColumnsCsv;
        private final String referencedTable;
        private final String referencedColumnsCsv;
        private final String onDelete;
        private final String onUpdate;
        private final String constraintName; // pode estar vazio

        public ForeignKeyOperation(Kind kind,
                                   String baseTable,
                                   String baseColumnsCsv,
                                   String referencedTable,
                                   String referencedColumnsCsv,
                                   String onDelete,
                                   String onUpdate,
                                   String constraintName) {
            this.kind = kind;
            this.baseTable = baseTable;
            this.baseColumnsCsv = baseColumnsCsv;
            this.referencedTable = referencedTable;
            this.referencedColumnsCsv = referencedColumnsCsv;
            this.onDelete = onDelete;
            this.onUpdate = onUpdate;
            this.constraintName = constraintName;
        }

        public Kind getKind() { return kind; }
        public String getBaseTable() { return baseTable; }
        public String getBaseColumnsCsv() { return baseColumnsCsv; }
        public String getReferencedTable() { return referencedTable; }
        public String getReferencedColumnsCsv() { return referencedColumnsCsv; }
        public String getOnDelete() { return onDelete; }
        public String getOnUpdate() { return onUpdate; }
        public String getConstraintName() { return constraintName; }

        @Override
        public String toString() {
            return "FK{" + kind + " " + baseTable + "(" + baseColumnsCsv + ") -> "
                    + referencedTable + "(" + referencedColumnsCsv + ")"
                    + ", onDelete=" + onDelete + ", onUpdate=" + onUpdate
                    + ", name=" + constraintName + "}";
        }
    }

    public static final class ExtractionResult {
        private final String yamlWithoutFkChanges;
        private final Map<String, List<ForeignKeyOperation>> operationsByTable;

        public ExtractionResult(String yamlWithoutFkChanges, Map<String, List<ForeignKeyOperation>> operationsByTable) {
            this.yamlWithoutFkChanges = yamlWithoutFkChanges;
            this.operationsByTable = operationsByTable;
        }
        public String getYamlWithoutFkChanges() { return yamlWithoutFkChanges; }
        public Map<String, List<ForeignKeyOperation>> getOperationsByTable() { return operationsByTable; }
    }

    // Blocos que nos interessam
    private static final String ADD_FK_HEADER = "- addForeignKeyConstraint:";
    private static final String DROP_FK_HEADER = "- dropForeignKeyConstraint:";

    // Campos que tentamos capturar
    private static final Pattern KV_PATTERN = Pattern.compile("^\\s*([a-zA-Z0-9_]+)\\s*:\\s*(.*?)\\s*$");

    public ExtractionResult extractAndRemoveFkChanges(String originalYaml) {
        List<String> lines = new ArrayList<>(Arrays.asList(originalYaml.split("\n", -1)));
        List<String> cleaned = new ArrayList<>(lines.size());

        Map<String, List<ForeignKeyOperation>> map = new LinkedHashMap<>();

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);

            if (line.contains(ADD_FK_HEADER) || line.contains(DROP_FK_HEADER)) {
                // Captura o bloco do change atual
                int blockStart = i;
                int blockEnd = findBlockEnd(lines, i + 1);
                List<String> block = lines.subList(blockStart, blockEnd);

                boolean isAdd = line.contains(ADD_FK_HEADER);
                ForeignKeyOperation op = parseForeignKeyBlock(block, isAdd);
                if (op != null && op.getBaseTable() != null) {
                    map.computeIfAbsent(op.getBaseTable(), k -> new ArrayList<>()).add(op);
                }

                // NÃO copiamos esse bloco para o YAML final (removemos o change)
                i = blockEnd - 1;
                continue;
            }

            cleaned.add(line);
        }

        // Se retiramos linhas extras em branco (por estética)
        String yamlOut = pruneEmptyChangeSets(String.join("\n", cleaned));
        return new ExtractionResult(yamlOut, map);
    }

    private static int findBlockEnd(List<String> lines, int from) {
        for (int i = from; i < lines.size(); i++) {
            String t = lines.get(i).trim();
            if (t.startsWith("- ")) return i;
        }
        return lines.size();
    }

    private ForeignKeyOperation parseForeignKeyBlock(List<String> block, boolean isAdd) {
        Map<String, String> kv = new LinkedHashMap<>();
        for (int i = 1; i < block.size(); i++) {
            String l = block.get(i);
            Matcher m = KV_PATTERN.matcher(l);
            if (m.matches()) {
                String key = m.group(1).trim();
                String value = stripQuotes(m.group(2).trim());
                kv.put(key, value);
            }
        }

        String baseTable = kv.getOrDefault("baseTableName", null);
        String baseCols = kv.getOrDefault("baseColumnNames", null);
        String refTable = kv.getOrDefault("referencedTableName", null);
        String refCols = kv.getOrDefault("referencedColumnNames", null);
        String onDelete = kv.getOrDefault("onDelete", null);
        String onUpdate = kv.getOrDefault("onUpdate", null);
        String constraintName = kv.getOrDefault("constraintName", null);

        if (baseTable == null || baseTable.isBlank()) return null;

        return new ForeignKeyOperation(
                isAdd ? ForeignKeyOperation.Kind.ADD : ForeignKeyOperation.Kind.DROP,
                baseTable, nullToEmpty(baseCols), nullToEmpty(refTable), nullToEmpty(refCols),
                nullToEmpty(onDelete), nullToEmpty(onUpdate), nullToEmpty(constraintName)
        );
    }

    private static String stripQuotes(String v) {
        if (v == null) return null;
        if ((v.startsWith("'") && v.endsWith("'")) || (v.startsWith("\"") && v.endsWith("\""))) {
            return v.substring(1, v.length() - 1);
        }
        return v;
    }
    private static String nullToEmpty(String s) { return s == null ? "" : s; }

    private static String pruneEmptyChangeSets(String yaml) {
        // Remove changeSets vazios que ficaram só com "- changeSet:" e nada útil dentro.
        // Heurística simples para não deixar "lixo".
        String[] arr = yaml.split("\n");
        List<String> out = new ArrayList<>();
        boolean skipping = false;
        List<String> buffer = new ArrayList<>();
        for (String l : arr) {
            if (l.trim().equals("- changeSet:")) {
                if (!buffer.isEmpty()) out.addAll(buffer);
                buffer.clear();
                buffer.add(l);
                skipping = true;
            } else if (skipping && l.trim().startsWith("- ")) {
                // próximo item: avalia buffer
                if (buffer.size() <= 1) {
                    // vazio => não adiciona
                } else {
                    out.addAll(buffer);
                }
                buffer.clear();
                out.add(l);
                skipping = false;
            } else if (skipping) {
                buffer.add(l);
            } else {
                out.add(l);
            }
        }
        if (skipping && buffer.size() > 1) {
            out.addAll(buffer);
        }
        return String.join("\n", out);
    }
}
