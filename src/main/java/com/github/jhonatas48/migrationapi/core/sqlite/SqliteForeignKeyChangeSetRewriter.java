package com.github.jhonatas48.migrationapi.core.sqlite;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reescreve changeSets do Liquibase para tornar compatível com SQLite:
 * - Remove "addForeignKeyConstraint"
 * - Insere sequência "ALTER RENAME -> CREATE TABLE (com FK inline) -> INSERT ... SELECT -> DROP old"
 *
 * Observações / Escopo:
 * 1) Este rewriter reconstrói o CREATE TABLE a partir dos blocos "createTable" que estejam
 *    no mesmo YAML. Ele captura: name, type e flags simples (primaryKey, nullable).
 * 2) Se não encontrar a definição da tabela de base no YAML, mantém o changeSet original
 *    (fallback seguro) para evitar gerar DDL inválido.
 * 3) Não tenta inferir ON UPDATE/DELETE; utiliza o padrão do SQLite (NO ACTION).
 * 4) Nomes são tratados literalmente; ajuste se precisar quoting.
 */
public class SqliteForeignKeyChangeSetRewriter {

    private static final String CHANGESET_START = "- changeSet:";
    private static final String CHANGES_KEY = "changes:";
    private static final Pattern CREATE_TABLE_START =
            Pattern.compile("^\\s*-\\s*createTable:\\s*$");
    private static final Pattern CREATE_TABLE_NAME =
            Pattern.compile("^\\s*tableName:\\s*(\\S+)\\s*$");
    private static final Pattern COLUMN_START =
            Pattern.compile("^\\s*-\\s*column:\\s*$");
    private static final Pattern COLUMN_NAME =
            Pattern.compile("^\\s*name:\\s*(\\S+)\\s*$");
    private static final Pattern COLUMN_TYPE =
            Pattern.compile("^\\s*type:\\s*(\\S+)\\s*$");
    private static final Pattern COLUMN_CONS_PRIMARY =
            Pattern.compile("^\\s*primaryKey:\\s*true\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern COLUMN_CONS_NULLABLE_FALSE =
            Pattern.compile("^\\s*nullable:\\s*false\\s*$", Pattern.CASE_INSENSITIVE);

    private static final Pattern ADD_FK_START =
            Pattern.compile("^\\s*-\\s*addForeignKeyConstraint:\\s*$");
    private static final Pattern ADD_FK_BASE_TABLE =
            Pattern.compile("^\\s*baseTableName:\\s*(\\S+)\\s*$");
    private static final Pattern ADD_FK_BASE_COLUMNS =
            Pattern.compile("^\\s*baseColumnNames:\\s*(\\S+)\\s*$");
    private static final Pattern ADD_FK_REF_TABLE =
            Pattern.compile("^\\s*referencedTableName:\\s*(\\S+)\\s*$");
    private static final Pattern ADD_FK_REF_COLUMNS =
            Pattern.compile("^\\s*referencedColumnNames:\\s*(\\S+)\\s*$");
    private static final Pattern ADD_FK_NAME =
            Pattern.compile("^\\s*constraintName:\\s*(\\S+)\\s*$");

    public String rewrite(String originalYaml) {
        List<String> lines = new ArrayList<>(Arrays.asList(originalYaml.split("\n", -1)));

        // 1) Índice de tabelas definidas via createTable neste YAML
        Map<String, TableDef> tableDefs = collectTableDefinitions(lines);

        // 2) Percorre changesets e reescreve onde houver addForeignKeyConstraint
        List<String> result = new ArrayList<>();
        int i = 0;
        while (i < lines.size()) {
            String line = lines.get(i);

            if (line.trim().equals(CHANGESET_START)) {
                int changeSetStart = i;
                int changeSetEnd = findChangeSetEnd(lines, i + 1);

                List<String> rewritten = rewriteChangeSet(lines.subList(changeSetStart, changeSetEnd), tableDefs);
                result.addAll(rewritten);
                i = changeSetEnd;
                continue;
            }

            result.add(line);
            i++;
        }

        return String.join("\n", result);
    }

    /* ===================== core ===================== */

    private List<String> rewriteChangeSet(List<String> changeSetBlock, Map<String, TableDef> tableDefs) {
        // Detecta se dentro deste changeSet há um addForeignKeyConstraint
        int changesIndex = indexOfLineTrim(changeSetBlock, CHANGES_KEY);
        if (changesIndex < 0) return new ArrayList<>(changeSetBlock);

        // Copia e iremos operar sobre a sublista de changes
        List<String> before = changeSetBlock.subList(0, changesIndex + 1);
        List<String> changes = new ArrayList<>();
        List<String> after = new ArrayList<>();

        // Split simplista: tudo após "changes:" pertence aos changes
        // (até terminar o bloco do changeSet; aqui, changeSetBlock não inclui próximo changeSet)
        changes.addAll(changeSetBlock.subList(changesIndex + 1, changeSetBlock.size()));

        // Vamos percorrer os changes uma vez: copiar todos os changes “normais”,
        // porém quando encontrarmos addForeignKeyConstraint, substituímos por bloco SQL de recriação.
        List<String> outChanges = new ArrayList<>();
        int i = 0;
        while (i < changes.size()) {
            String line = changes.get(i);

            if (ADD_FK_START.matcher(line).matches()) {
                // Coleta o bloco do addForeignKeyConstraint
                int fkStart = i;
                int fkEnd = findItemEnd(changes, i + 1);

                ForeignKeyDef fk = readForeignKey(changes.subList(fkStart, fkEnd));
                if (fk.isValid() && tableDefs.containsKey(fk.baseTable)) {
                    TableDef base = tableDefs.get(fk.baseTable);

                    // (1) remove o addForeignKeyConstraint
                    // (2) injeta bloco SQL de recriação
                    outChanges.addAll(buildSqlRecreateBlock(base, fk));
                } else {
                    // não sabemos recriar — mantém original para transparência
                    outChanges.addAll(changes.subList(fkStart, fkEnd));
                }
                i = fkEnd;
                continue;
            }

            // Mantém o change original
            outChanges.add(line);
            i++;
        }

        // Remonta bloco final do changeSet
        List<String> rewritten = new ArrayList<>(before);
        rewritten.addAll(outChanges);
        rewritten.addAll(after);
        return rewritten;
    }

    private List<String> buildSqlRecreateBlock(TableDef base, ForeignKeyDef fk) {
        // Gera:
        // - sql: PRAGMA foreign_keys = OFF
        // - sql: ALTER TABLE <base> RENAME TO <base>__old
        // - sql: CREATE TABLE <base>(<columns>, FOREIGN KEY(<cols>) REFERENCES <ref>(<refcols>))
        // - sql: INSERT INTO <base>(<cols...>) SELECT <cols...> FROM <base>__old
        // - sql: DROP TABLE <base>__old
        // - sql: PRAGMA foreign_keys = ON
        //
        // Observação: FK name não é relevante no SQLite para PRAGMA; omitimos.
        String indent = "      "; // alinhado ao nível dos items em 'changes:'

        String oldName = base.tableName + "__old";
        String colList = String.join(", ", base.columnNames());

        String create = "CREATE TABLE " + base.tableName + " (\n" +
                "  " + String.join(",\n  ", base.columnDefs()) + ",\n" +
                "  FOREIGN KEY (" + String.join(", ", fk.baseColumns) + ") REFERENCES " +
                fk.referencedTable + " (" + String.join(", ", fk.referencedColumns) + ")\n" +
                ");";

        StringBuilder sb = new StringBuilder();
        sb.append("PRAGMA foreign_keys = OFF;\n");
        sb.append("ALTER TABLE ").append(base.tableName).append(" RENAME TO ").append(oldName).append(";\n");
        sb.append(create).append("\n");
        sb.append("INSERT INTO ").append(base.tableName)
                .append(" (").append(colList).append(")")
                .append(" SELECT ").append(colList).append(" FROM ").append(oldName).append(";\n");
        sb.append("DROP TABLE ").append(oldName).append(";\n");
        sb.append("PRAGMA foreign_keys = ON;");

        // Coloca em um único change "- sql:"
        List<String> block = new ArrayList<>();
        block.add(indent + "- sql:");
        block.add(indent + "    sql: |");
        for (String sqlLine : sb.toString().split("\n")) {
            block.add(indent + "      " + sqlLine);
        }
        return block;
    }

    /* ===================== parsing helpers ===================== */

    private Map<String, TableDef> collectTableDefinitions(List<String> lines) {
        Map<String, TableDef> map = new LinkedHashMap<>();

        int i = 0;
        while (i < lines.size()) {
            String line = lines.get(i);

            if (CREATE_TABLE_START.matcher(line.trim()).matches()) {
                int blockStart = i;
                int blockEnd = findItemEnd(lines, i + 1);

                TableDef def = parseCreateTable(lines.subList(blockStart, blockEnd));
                if (def != null) {
                    map.put(def.tableName, def);
                }
                i = blockEnd;
                continue;
            }
            i++;
        }

        return map;
    }

    private TableDef parseCreateTable(List<String> block) {
        String tableName = null;
        List<ColumnDef> columns = new ArrayList<>();

        int i = 0;
        while (i < block.size()) {
            String line = block.get(i);

            Matcher tname = CREATE_TABLE_NAME.matcher(line.trim());
            if (tname.matches()) {
                tableName = strip(tname.group(1));
                i++;
                continue;
            }

            if (COLUMN_START.matcher(line.trim()).matches()) {
                int colStart = i;
                int colEnd = findItemEnd(block, i + 1);
                ColumnDef col = parseColumn(block.subList(colStart, colEnd));
                if (col != null) columns.add(col);
                i = colEnd;
                continue;
            }

            i++;
        }

        if (tableName == null || columns.isEmpty()) return null;
        return new TableDef(tableName, columns);
    }

    private ColumnDef parseColumn(List<String> colBlock) {
        String name = null;
        String type = null;
        boolean primary = false;
        Boolean nullable = null;

        for (String l : colBlock) {
            String t = l.trim();

            Matcher n = COLUMN_NAME.matcher(t);
            if (n.matches()) {
                name = strip(n.group(1));
                continue;
            }
            Matcher tp = COLUMN_TYPE.matcher(t);
            if (tp.matches()) {
                type = strip(tp.group(1));
                continue;
            }
            if (COLUMN_CONS_PRIMARY.matcher(t).matches()) {
                primary = true;
                continue;
            }
            if (COLUMN_CONS_NULLABLE_FALSE.matcher(t).matches()) {
                nullable = Boolean.FALSE;
            }
        }

        if (name == null || type == null) return null;
        return new ColumnDef(name, type, primary, nullable != null && !nullable);
    }

    private ForeignKeyDef readForeignKey(List<String> fkBlock) {
        String baseTable = null;
        List<String> baseCols = null;
        String refTable = null;
        List<String> refCols = null;
        String name = null;

        for (String l : fkBlock) {
            String t = l.trim();

            Matcher m;
            if ((m = ADD_FK_BASE_TABLE.matcher(t)).matches()) baseTable = strip(m.group(1));
            else if ((m = ADD_FK_BASE_COLUMNS.matcher(t)).matches()) baseCols = splitCsv(strip(m.group(1)));
            else if ((m = ADD_FK_REF_TABLE.matcher(t)).matches()) refTable = strip(m.group(1));
            else if ((m = ADD_FK_REF_COLUMNS.matcher(t)).matches()) refCols = splitCsv(strip(m.group(1)));
            else if ((m = ADD_FK_NAME.matcher(t)).matches()) name = strip(m.group(1));
        }
        return new ForeignKeyDef(baseTable, baseCols, refTable, refCols, name);
    }

    private static int findChangeSetEnd(List<String> lines, int from) {
        for (int i = from; i < lines.size(); i++) {
            String t = lines.get(i).trim();
            if (t.equals(CHANGESET_START)) {
                return i;
            }
        }
        return lines.size();
    }

    private static int findItemEnd(List<String> lines, int from) {
        for (int i = from; i < lines.size(); i++) {
            String t = lines.get(i).trim();
            if (t.startsWith("- ")) return i; // próximo item no mesmo nível
        }
        return lines.size();
    }

    private static int indexOfLineTrim(List<String> lines, String needle) {
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).trim().equals(needle)) return i;
        }
        return -1;
    }

    private static String strip(String s) {
        if (s == null) return null;
        // remove aspas simples ou duplas ao redor
        if ((s.startsWith("'") && s.endsWith("'")) || (s.startsWith("\"") && s.endsWith("\""))) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private static List<String> splitCsv(String csv) {
        String[] parts = csv.split(",");
        List<String> list = new ArrayList<>(parts.length);
        for (String p : parts) {
            String t = p.trim();
            if (!t.isEmpty()) list.add(t);
        }
        return list;
    }

    /* ===================== DTOs ===================== */

    private static final class TableDef {
        final String tableName;
        final List<ColumnDef> columns;
        TableDef(String tableName, List<ColumnDef> columns) {
            this.tableName = tableName;
            this.columns = columns;
        }

        List<String> columnNames() {
            List<String> names = new ArrayList<>(columns.size());
            for (ColumnDef c : columns) names.add(c.name);
            return names;
        }

        List<String> columnDefs() {
            List<String> defs = new ArrayList<>(columns.size());
            for (ColumnDef c : columns) {
                StringBuilder sb = new StringBuilder();
                sb.append(c.name).append(" ").append(c.type);
                if (c.primaryKey) sb.append(" PRIMARY KEY");
                if (c.notNull) sb.append(" NOT NULL");
                defs.add(sb.toString());
            }
            return defs;
        }
    }

    private static final class ColumnDef {
        final String name;
        final String type;
        final boolean primaryKey;
        final boolean notNull;
        ColumnDef(String name, String type, boolean primaryKey, boolean notNull) {
            this.name = name;
            this.type = type;
            this.primaryKey = primaryKey;
            this.notNull = notNull;
        }
    }

    private static final class ForeignKeyDef {
        final String baseTable;
        final List<String> baseColumns;
        final String referencedTable;
        final List<String> referencedColumns;
        final String name; // ignorado no SQLite

        ForeignKeyDef(String baseTable, List<String> baseColumns, String referencedTable, List<String> referencedColumns, String name) {
            this.baseTable = baseTable;
            this.baseColumns = baseColumns;
            this.referencedTable = referencedTable;
            this.referencedColumns = referencedColumns;
            this.name = name;
        }
        boolean isValid() {
            return baseTable != null && referencedTable != null
                    && baseColumns != null && !baseColumns.isEmpty()
                    && referencedColumns != null && !referencedColumns.isEmpty();
        }
    }
}
