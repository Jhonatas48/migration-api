
package com.github.jhonatas48.migrationapi.core;

import com.github.jhonatas48.migrationapi.MigrationProperties;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SqliteChangeSetRewriter {

    private final MigrationProperties migrationProperties;

    public SqliteChangeSetRewriter(MigrationProperties migrationProperties) {
        this.migrationProperties = migrationProperties;
    }

    public String rewrite(String yaml) {
        if (!migrationProperties.isSqliteRewriteEnabled()) return yaml;

        List<String> lines = new ArrayList<>(Arrays.asList(yaml.split("\n", -1)));
        List<String> out = new ArrayList<>(lines.size());
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            // Heurística mínima: mantemos as operações para que o Liquibase faça a recriação.
            out.add(line);
        }
        return String.join("\n", out);
    }
}
