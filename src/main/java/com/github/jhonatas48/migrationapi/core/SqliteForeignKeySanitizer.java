package com.github.jhonatas48.migrationapi.core;

import com.github.jhonatas48.migrationapi.core.audit.HashUtils;
import com.github.jhonatas48.migrationapi.core.audit.MigrationAuditService;
import com.github.jhonatas48.migrationapi.core.sqlite.SqliteTableRebuilder;
import com.github.jhonatas48.migrationapi.core.sqlite.SqliteTableRebuilder.ForeignKeySpec;
import com.github.jhonatas48.migrationapi.core.yaml.YamlForeignKeyExtractor;
import com.github.jhonatas48.migrationapi.core.yaml.YamlForeignKeyExtractor.ForeignKeyOperation;
import com.github.jhonatas48.migrationapi.core.yaml.YamlForeignKeyExtractor.ExtractionResult;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Para SQLite:
 * - Remove do YAML as operações de FK (add/drop)
 * - Para cada tabela com mudanças, reconstrói fisicamente via SqliteTableRebuilder
 * - Garante idempotência via MigrationAuditService (hash do "plano" de FKs)
 */
public class SqliteForeignKeySanitizer {

    private final DataSource dataSource;
    private final MigrationAuditService auditService;

    public SqliteForeignKeySanitizer(DataSource dataSource, MigrationAuditService auditService) {
        this.dataSource = dataSource;
        this.auditService = auditService;
    }

    public boolean isSqlite() {
        try (Connection c = dataSource.getConnection()) {
            String product = c.getMetaData().getDatabaseProductName();
            return product != null && product.toLowerCase(Locale.ROOT).contains("sqlite");
        } catch (SQLException e) {
            throw new IllegalStateException("Não foi possível detectar o DBMS", e);
        }
    }

    public SanitizationResult sanitizeAndApplyRebuilds(String originalYaml) throws Exception {
        if (!isSqlite()) {
            return new SanitizationResult(originalYaml, false);
        }

        YamlForeignKeyExtractor extractor = new YamlForeignKeyExtractor();
        ExtractionResult extraction = extractor.extractAndRemoveFkChanges(originalYaml);

        Map<String, List<ForeignKeyOperation>> opsByTable = extraction.getOperationsByTable();
        if (opsByTable.isEmpty()) {
            return new SanitizationResult(originalYaml, false);
        }

        auditService.ensureTable();

        // Monta um "plano" textual determinístico para hash
        String normalizedPlan = buildNormalizedPlan(opsByTable);
        String planHash = HashUtils.sha256Hex(normalizedPlan);

        if (!auditService.wasAlreadyApplied(planHash)) {
            // Para cada tabela, montar "adds" e "drops" e chamar o rebuilder
            SqliteTableRebuilder rebuilder = new SqliteTableRebuilder(dataSource);

            for (Map.Entry<String, List<ForeignKeyOperation>> e : opsByTable.entrySet()) {
                String table = e.getKey();
                List<ForeignKeyOperation> list = e.getValue();

                List<ForeignKeySpec> addSpecs = list.stream()
                        .filter(op -> op.getKind() == ForeignKeyOperation.Kind.ADD)
                        .map(SqliteForeignKeySanitizer::toSpec)
                        .collect(Collectors.toList());

                List<ForeignKeySpec> dropSpecs = list.stream()
                        .filter(op -> op.getKind() == ForeignKeyOperation.Kind.DROP)
                        .map(SqliteForeignKeySanitizer::toSpec)
                        .collect(Collectors.toList());

                rebuilder.rebuildTableApplyingForeignKeyChanges(table, addSpecs, dropSpecs);
            }

            auditService.recordApplied(planHash, normalizedPlan);
        }

        // Retorna YAML sem changes de FK (para Liquibase aplicar o resto)
        return new SanitizationResult(extraction.getYamlWithoutFkChanges(), true);
    }

    private static ForeignKeySpec toSpec(ForeignKeyOperation op) {
        return new ForeignKeySpec(
                op.getBaseColumnsCsv(),
                op.getReferencedTable(),
                op.getReferencedColumnsCsv(),
                op.getOnDelete(),
                op.getOnUpdate()
        );
    }

    private String buildNormalizedPlan(Map<String, List<ForeignKeyOperation>> opsByTable) {
        // Texto canônico para hash: por tabela, ADD/DROP ordenado por colunas
        StringBuilder sb = new StringBuilder();
        List<String> tables = new ArrayList<>(opsByTable.keySet());
        Collections.sort(tables, String.CASE_INSENSITIVE_ORDER);
        for (String t : tables) {
            sb.append("TABLE=").append(t).append("\n");
            List<ForeignKeyOperation> ops = new ArrayList<>(opsByTable.get(t));
            ops.sort(Comparator.comparing(ForeignKeyOperation::getKind)
                    .thenComparing(o -> o.getBaseColumnsCsv().toLowerCase(Locale.ROOT)));
            for (ForeignKeyOperation op : ops) {
                sb.append(op.getKind()).append(" ")
                        .append(op.getBaseColumnsCsv()).append(" -> ")
                        .append(op.getReferencedTable()).append("(")
                        .append(op.getReferencedColumnsCsv()).append(")")
                        .append(op.getOnDelete().isBlank() ? "" : " DEL=" + op.getOnDelete())
                        .append(op.getOnUpdate().isBlank() ? "" : " UPD=" + op.getOnUpdate())
                        .append("\n");
            }
        }
        return sb.toString();
    }

    public static final class SanitizationResult {
        private final String yamlForLiquibase;
        private final boolean altered;

        public SanitizationResult(String yamlForLiquibase, boolean altered) {
            this.yamlForLiquibase = yamlForLiquibase;
            this.altered = altered;
        }
        public String getYamlForLiquibase() { return yamlForLiquibase; }
        public boolean wasAltered() { return altered; }
    }
}
