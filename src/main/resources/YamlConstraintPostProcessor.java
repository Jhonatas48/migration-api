
package com.github.jhonatas48.migrationapi.core;

import com.github.jhonatas48.migrationapi.MigrationProperties;

import java.util.*;

public class YamlConstraintPostProcessor {

    private final MigrationProperties properties;

    public YamlConstraintPostProcessor(MigrationProperties properties) {
        this.properties = properties;
    }

    public String process(String yaml) {
        if (!properties.isAutoNameConstraints()) return yaml;
        List<String> lines = new ArrayList<>(Arrays.asList(yaml.split("\n", -1)));
        boolean changed = false;

        for (int i = 0; i < lines.size(); i++) {
            String current = lines.get(i);
            if (current.contains("- addForeignKeyConstraint:") || current.contains("- dropForeignKeyConstraint:")) {
                int start = i;
                int end = findBlockEnd(lines, i + 1);
                Map<String, String> fields = readKeyValues(lines, start + 1, end);
                if (!fields.containsKey("constraintName")) {
                    String baseTable = fields.getOrDefault("baseTableName", "table");
                    String baseColumns = fields.getOrDefault("baseColumnNames", "col");
                    String refTable = fields.get("referencedTableName");
                    String autoName = buildForeignKeyName(baseTable, baseColumns, refTable);
                    String indent = leadingSpacesOf(lines.get(start + 1));
                    lines.add(start + 1, indent + "constraintName: " + autoName);
                    changed = true;
                }
                i = end;
            }
        }
        return changed ? String.join("\n", lines) : yaml;
    }

    private static int findBlockEnd(List<String> lines, int from) {
        for (int i = from; i < lines.size(); i++) {
            String t = lines.get(i).trim();
            if (t.startsWith("- ")) return i;
        }
        return lines.size();
    }

    private static Map<String, String> readKeyValues(List<String> lines, int start, int end) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = start; i < end && i < lines.size(); i++) {
            String raw = lines.get(i);
            if (raw == null) continue;
            String trimmed = raw.trim();
            int idx = trimmed.indexOf(':');
            if (idx > 0) {
                String key = trimmed.substring(0, idx).trim();
                String val = trimmed.substring(idx + 1).trim();
                val = stripQuotes(val);
                if (!key.isEmpty()) map.put(key, val);
            }
        }
        return map;
    }

    private static String stripQuotes(String v) {
        if (v == null) return null;
        if ((v.startsWith("'") && v.endsWith("'")) || (v.startsWith("\"") && v.endsWith("\""))) {
            return v.substring(1, v.length() - 1);
        }
        return v;
    }

    private static String leadingSpacesOf(String s) {
        if (s == null) return "";
        int i = 0;
        while (i < s.length() && Character.isWhitespace(s.charAt(i)) && s.charAt(i) != '\n' && s.charAt(i) != '\r') i++;
        return s.substring(0, i);
    }

    private static String buildForeignKeyName(String baseTable, String baseColumnsCsv, String ref) {
        String base = slug(baseTable);
        String cols = slug(baseColumnsCsv.replace(",", "_"));
        String refp = (ref == null || ref.isBlank()) ? null : slug(ref);
        String raw = (refp == null) ? String.format("fk_%s_%s", base, cols) : String.format("fk_%s_%s__%s", base, cols, refp);
        return raw.length() <= 60 ? raw : raw.substring(0, 60);
    }

    private static String slug(String v) {
        if (v == null) return "v";
        String lower = v.toLowerCase(Locale.ROOT);
        StringBuilder b = new StringBuilder(lower.length());
        for (int i = 0; i < lower.length(); i++) {
            char c = lower.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') b.append(c);
            else b.append('_');
        }
        String n = b.toString().replaceAll("_+", "_"); 
        if (n.startsWith("_")) n = n.substring(1);
        return n.isEmpty() ? "v" : n;
    }
}
