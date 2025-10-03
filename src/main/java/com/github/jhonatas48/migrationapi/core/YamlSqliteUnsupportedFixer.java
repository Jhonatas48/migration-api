package com.github.jhonatas48.migrationapi.core;


import java.util.*;

/**
 * Reescreve changes YAML para evitar erro de validação no SQLite,
 * envolvendo changes incompatíveis com uma preCondition "NOT DBMS sqlite".
 *
 * O objetivo é:
 * - Continuar aplicando normalmente em bancos que suportam (Postgres/MySQL/etc.)
 * - No SQLite, marcar como "MARK_RAN" (não executar) para não quebrar a validação.
 *
 * Se quiser a versão que recria tabelas para embutir FKs no SQLite,
 * implemento como um rewriter separado (é bem mais invasivo).
 */
public class YamlSqliteUnsupportedFixer {

    // Quais changes vamos blindar por padrão
    private static final Set<String> CHANGE_KEYS_TO_GUARD = Set.of(
            "- addForeignKeyConstraint:",
            "- addUniqueConstraint:",
            "- dropForeignKeyConstraint:" // mantém porque o SQLite não tem "drop constraint"
    );

    public String process(String yaml) {
        List<String> lines = new ArrayList<>(Arrays.asList(yaml.split("\n", -1)));
        List<String> output = new ArrayList<>(lines.size() + 32);

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);

            // Detecta início de um changeSet
            if (line.trim().equals("- changeSet:")) {
                int csStart = i;
                int csEnd = findChangeSetEnd(lines, csStart + 1);

                List<String> changeSetBlock = lines.subList(csStart, csEnd);
                List<String> fixedBlock = guardUnsupportedForSqlite(changeSetBlock);

                output.addAll(fixedBlock);
                i = csEnd - 1;
            } else {
                output.add(line);
            }
        }

        return String.join("\n", output);
    }

    private static int findChangeSetEnd(List<String> lines, int from) {
        for (int i = from; i < lines.size(); i++) {
            String t = lines.get(i).trim();
            if (t.equals("- changeSet:")) return i;
        }
        return lines.size();
    }

    /**
     * Se o changeSet contém um dos changes incompatíveis com SQLite,
     * injeta preConditions:
     *
     * preConditions:
     *   onFail: MARK_RAN
     *   - not:
     *       - dbms:
     *           type: sqlite
     */
    private static List<String> guardUnsupportedForSqlite(List<String> changeSetBlock) {
        boolean containsUnsupported = changeSetBlock.stream()
                .map(String::trim)
                .anyMatch(line -> CHANGE_KEYS_TO_GUARD.stream().anyMatch(line::startsWith));

        if (!containsUnsupported) {
            return new ArrayList<>(changeSetBlock);
        }

        // Já tem preConditions? Se já tiver, não duplicamos. (Heurística simples)
        boolean alreadyHasPreconditions = changeSetBlock.stream()
                .anyMatch(s -> s.trim().startsWith("preConditions:"));

        if (alreadyHasPreconditions) {
            return new ArrayList<>(changeSetBlock);
        }

        // Inserimos preConditions logo após "- changeSet:"
        List<String> out = new ArrayList<>(changeSetBlock.size() + 8);
        for (int i = 0; i < changeSetBlock.size(); i++) {
            String l = changeSetBlock.get(i);
            out.add(l);
            if (i == 0) {
                String indent = leadingSpacesOf(nextNonEmpty(changeSetBlock, 1));
                // bloco de preconditions
                out.add(indent + "preConditions:");
                out.add(indent + "  onFail: MARK_RAN");
                out.add(indent + "  - not:");
                out.add(indent + "      - dbms:");
                out.add(indent + "          type: sqlite");
            }
        }
        return out;
        // Observação: isso evita erro de validação e faz o Liquibase “pular” esses changes só no SQLite.
    }

    private static String nextNonEmpty(List<String> block, int start) {
        for (int i = start; i < block.size(); i++) {
            String s = block.get(i);
            if (s != null && !s.isBlank()) return s;
        }
        return "  ";
    }

    private static String leadingSpacesOf(String s) {
        if (s == null) return "";
        int i = 0;
        while (i < s.length() && Character.isWhitespace(s.charAt(i)) && s.charAt(i) != '\n' && s.charAt(i) != '\r') i++;
        return s.substring(0, i);
    }
}
