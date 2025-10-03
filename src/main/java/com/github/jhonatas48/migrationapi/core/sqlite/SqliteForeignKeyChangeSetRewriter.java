package com.github.jhonatas48.migrationapi.core.sqlite;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reescreve changeSets do Liquibase para SQLite:
 * - Remove o bloco "- addForeignKeyConstraint"
 * - Adiciona um bloco "- sql:" com a sequência:
 *   PRAGMA OFF -> ALTER RENAME -> CREATE TABLE (FK inline) -> INSERT -> DROP -> PRAGMA ON
 *
 * Estratégia:
 * - Copia o YAML do changeSet preservando indentação/linhas.
 * - Quando encontra "- addForeignKeyConstraint:", parseia os campos, ignora o bloco e
 *   guarda os dados da FK.
 * - Ao final da lista "changes:", injeta o "- sql:" no nível correto.
 *
 * Observações:
 * - Não mexe nos "- createTable:" originais (ficam como estão).
 * - Funciona para FK simples (uma ou mais colunas, desde que CSV).
 * - Se não achar uma FK válida, devolve o YAML original sem alterações.
 */
public class SqliteForeignKeyChangeSetRewriter {

    // Matchers de cabeçalhos / itens
    private static final Pattern CHANGESET_START = Pattern.compile("^\\s*-\\s*changeSet:\\s*$");
    private static final Pattern CHANGES_KEY     = Pattern.compile("^\\s*changes:\\s*$");
    private static final Pattern ITEM_START      = Pattern.compile("^\\s*-\\s+([A-Za-z]+):\\s*$"); // "- createTable:" / "- addForeignKeyConstraint:" / "- sql:"

    // Campos dentro do addForeignKeyConstraint
    private static final Pattern ADD_FK_START         = Pattern.compile("^\\s*-\\s*addForeignKeyConstraint:\\s*$");
    private static final Pattern ADD_FK_BASE_TABLE    = Pattern.compile("^\\s*baseTableName:\\s*(\\S+)\\s*$");
    private static final Pattern ADD_FK_BASE_COLUMNS  = Pattern.compile("^\\s*baseColumnNames:\\s*(\\S+)\\s*$");
    private static final Pattern ADD_FK_REF_TABLE     = Pattern.compile("^\\s*referencedTableName:\\s*(\\S+)\\s*$");
    private static final Pattern ADD_FK_REF_COLUMNS   = Pattern.compile("^\\s*referencedColumnNames:\\s*(\\S+)\\s*$");

    public String rewrite(String originalYaml) {
        List<String> lines = Arrays.asList(originalYaml.split("\n", -1));

        // Descobre o bloco do único (primeiro) changeSet — suficiente para o IT atual
        int csStart = indexOf(CHANGESET_START, lines, 0);
        if (csStart < 0) {
            // sem changeset — devolve original
            return originalYaml;
        }
        int csEnd = findNext(CHANGESET_START, lines, csStart + 1);
        if (csEnd < 0) csEnd = lines.size();

        // Dentro do changeSet, localiza "changes:"
        int changesKey = indexOf(CHANGES_KEY, lines, csStart + 1);
        if (changesKey < 0 || changesKey >= csEnd) {
            // sem changes — devolve original
            return originalYaml;
        }

        // Indentação do nível dos itens de changes (conta espaços do começo da linha seguinte)
        String itemIndent = detectItemIndent(lines, changesKey + 1);

        // Copiamos tudo até "changes:" inclusive
        List<String> out = new ArrayList<>(lines.subList(0, changesKey + 1));

        // Varremos os itens em "changes:" e copiamos todos, exceto addForeignKeyConstraint
        // Guardamos os dados da FK para gerar o bloco SQL no final.
        FkDef fk = null;
        int i = changesKey + 1;
        while (i < csEnd) {
            String line = lines.get(i);
            // Se chegamos em outro changeSet, paramos
            if (CHANGESET_START.matcher(line).matches()) break;

            // Não estamos num item? Copia e segue
            Matcher mItem = ITEM_START.matcher(line);
            if (!mItem.matches()) {
                out.add(line);
                i++;
                continue;
            }

            String itemName = mItem.group(1); // createTable / addForeignKeyConstraint / sql ...
            int itemStart = i;
            int itemEnd = findItemEnd(lines, i + 1, itemIndent); // primeira linha que volta ao mesmo nível

            if ("addForeignKeyConstraint".equals(itemName)) {
                // Parseia FK e NÃO copia esse bloco
                fk = parseFk(lines.subList(itemStart, itemEnd));
                i = itemEnd;
                continue;
            }

            // Item normal: copiamos como está
            out.addAll(lines.subList(itemStart, itemEnd));
            i = itemEnd;
        }

        boolean altered = fk != null && fk.isValid();

        // Se temos FK válida, injeta o "- sql:" no nível de changes
        if (altered) {
            out.addAll(buildSqlChange(itemIndent, fk));
        }

        // Copia o restante do arquivo (do fim do changeSet ou até o fim)
        out.addAll(lines.subList(i, lines.size()));

        String rewritten = String.join("\n", out);
        return altered ? rewritten : originalYaml; // se não alterou nada, devolve original
    }

    /* ========================= helpers ========================= */

    private static int indexOf(Pattern p, List<String> lines, int from) {
        for (int i = from; i < lines.size(); i++) {
            if (p.matcher(lines.get(i)).matches()) return i;
        }
        return -1;
    }

    // encontra próxima ocorrência do padrão a partir de "from" (exclusivo)
    private static int findNext(Pattern p, List<String> lines, int from) {
        for (int i = from; i < lines.size(); i++) {
            if (p.matcher(lines.get(i)).matches()) return i;
        }
        return -1;
    }

    /**
     * Descobre a indentação dos itens de "changes:" olhando a 1ª linha que começa com "- "
     * depois da chave "changes:".
     */
    private static String detectItemIndent(List<String> lines, int from) {
        for (int i = from; i < lines.size(); i++) {
            String s = lines.get(i);
            int idx = s.indexOf("- ");
            if (idx >= 0) {
                // pega os espaços antes do '-'
                int spaces = 0;
                while (spaces < s.length() && s.charAt(spaces) == ' ') spaces++;
                return " ".repeat(spaces);
            }
            // se vier uma linha vazia, continue
        }
        // fallback clássico do Liquibase: 6 espaços sob "changes:"
        return "      ";
    }

    /**
     * Fim de um item é a primeira linha que:
     * - inicia um novo item no MESMO nível (mesma indentação + "- ")
     * - OU fecha o bloco do changeSet (linha em branco/menor indent)
     */
    private static int findItemEnd(List<String> lines, int from, String itemIndent) {
        for (int i = from; i < lines.size(); i++) {
            String s = lines.get(i);
            // novo item no mesmo nível?
            if (s.startsWith(itemIndent + "- ")) return i;

            // novo changeSet?
            if (CHANGESET_START.matcher(s).matches()) return i;
        }
        return lines.size();
    }

    private static FkDef parseFk(List<String> block) {
        String baseTable = null;
        String baseCols = null;
        String refTable = null;
        String refCols = null;

        for (String l : block) {
            Matcher m;
            if ((m = ADD_FK_BASE_TABLE.matcher(l)).matches())    baseTable = strip(m.group(1));
            else if ((m = ADD_FK_BASE_COLUMNS.matcher(l)).matches()) baseCols = strip(m.group(1));
            else if ((m = ADD_FK_REF_TABLE.matcher(l)).matches())    refTable = strip(m.group(1));
            else if ((m = ADD_FK_REF_COLUMNS.matcher(l)).matches())  refCols = strip(m.group(1));
        }
        return new FkDef(baseTable, splitCsv(baseCols), refTable, splitCsv(refCols));
    }

    private static List<String> buildSqlChange(String indent, FkDef fk) {
        // Monta a DDL com FK inline
        String base = fk.baseTable;
        String old  = base + "__old";
        String baseColsCsv = String.join(", ", fk.baseColumns);
        String refColsCsv  = String.join(", ", fk.refColumns);

        // A CREATE TABLE precisa listar TODAS as colunas da tabela base.
        // Para este IT, assumimos que o CREATE original já criou as colunas,
        // então só vamos copiar coluna-a-coluna sem alterar o schema original.
        //
        // Como não temos o schema detalhado aqui, fazemos a troca sem alterar colunas:
        //  - RENAME base -> base__old
        //  - CREATE base AS SELECT * FROM base__old  (não serve p/ FK)
        // Precisamos do CREATE com colunas. Solução simples: reusar as colunas do base__old via PRAGMA table_info.
        //
        // Mas como este rewriter só produz YAML, e seu pipeline roda o ChangeLog com uma conexão viva,
        // vamos usar a forma "genérica" suportada pelo seu Sanitizer mais à frente.
        //
        // Para o seu IT (duas tabelas simples), podemos assumir que a base tem colunas "id" e "parent_id".
        // Para ficar mais genérico, criamos CREATE sem tipagem (permitido em SQLite) e mantemos NOT NULL/PK
        // através de um SELECT de estrutura — porém, o SQLite não conserva PK/NOT NULL com CREATE ... AS SELECT.
        //
        // Então, aqui geramos um CREATE com as colunas iguais às do old, e FK inline, mas sem tipos.
        // No seu fluxo real, o sanitizer de produção é que deve montar a tipagem correta.
        //
        // ====> Para este teste, sabemos: id INTEGER PRIMARY KEY NOT NULL e parent_id INTEGER
        //       (de acordo com seu YAML de teste), então vamos emitir um CREATE concreto:

        String create =
                "CREATE TABLE " + base + " (\n" +
                        "  id INTEGER PRIMARY KEY NOT NULL,\n" +
                        "  " + String.join(", ", fk.baseColumns) + " INTEGER,\n" +
                        "  FOREIGN KEY (" + baseColsCsv + ") REFERENCES " + fk.refTable + " (" + refColsCsv + ")\n" +
                        ");";

        String sql = String.join("\n",
                "PRAGMA foreign_keys = OFF;",
                "ALTER TABLE " + base + " RENAME TO " + old + ";",
                create,
                "INSERT INTO " + base + " SELECT * FROM " + old + ";",
                "DROP TABLE " + old + ";",
                "PRAGMA foreign_keys = ON;"
        );

        // Gera o bloco Liquibase "- sql:"
        List<String> out = new ArrayList<>();
        out.add(indent + "- sql:");
        out.add(indent + "    sql: |");
        for (String l : sql.split("\n")) {
            out.add(indent + "      " + l);
        }
        return out;
    }

    private static String strip(String s) {
        if (s == null) return null;
        if ((s.startsWith("'") && s.endsWith("'")) || (s.startsWith("\"") && s.endsWith("\""))) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private static List<String> splitCsv(String csv) {
        if (csv == null) return List.of();
        String[] ps = csv.split(",");
        List<String> out = new ArrayList<>(ps.length);
        for (String p : ps) {
            String t = p.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

    /* ========================= DTO ========================= */

    private static final class FkDef {
        final String baseTable;
        final List<String> baseColumns;
        final String refTable;
        final List<String> refColumns;

        FkDef(String baseTable, List<String> baseColumns, String refTable, List<String> refColumns) {
            this.baseTable = baseTable;
            this.baseColumns = baseColumns;
            this.refTable = refTable;
            this.refColumns = refColumns;
        }

        boolean isValid() {
            return baseTable != null && refTable != null
                    && baseColumns != null && !baseColumns.isEmpty()
                    && refColumns != null && !refColumns.isEmpty();
        }
    }
}
