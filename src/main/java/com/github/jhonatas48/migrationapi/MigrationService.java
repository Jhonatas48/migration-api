package com.github.jhonatas48.migrationapi;

import liquibase.Scope;
import liquibase.command.CommandScope;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.CompositeResourceAccessor;
import liquibase.resource.DirectoryResourceAccessor;
import liquibase.resource.ResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Serviço de geração e aplicação de diffs Liquibase x Hibernate.
 *
 * PONTOS-CHAVE PARA SQLITE:
 * - Liquibase exige 'constraintName' em alguns changes (ex.: dropForeignKeyConstraint).
 * - O método 'postProcessYamlForConstraints' adiciona nomes automaticamente quando faltarem.
 * - Nomes são determinísticos (baseados em tabela/colunas) e adequados para evitar validação falhar.
 *
 * Observação: O SQLite tem limitações nativas (ex.: "drop constraint"). O Liquibase costuma
 * contornar recriando a tabela conforme o change. Este serviço garante que a validação não falhe;
 * em bancos “full”, o Liquibase geralmente fornece o nome da constraint por si só.
 */
public class MigrationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationService.class);

    private final MigrationProperties migrationProperties;

    @Value("${spring.datasource.url}")
    private String dataSourceUrl;

    @Value("${spring.datasource.username:}")
    private String dataSourceUsername;

    @Value("${spring.datasource.password:}")
    private String dataSourcePassword;

    private static final String LIQUIBASE_PLUGIN_PACKAGES_PROP = "liquibase.plugin.packages";
    private static final String HIBERNATE_PLUGIN_PACKAGE = "liquibase.ext.hibernate";

    private static final String CHANGE_LOG_FILE_PREFIX = "changelog-";
    private static final String CHANGE_LOG_FILE_SUFFIX = ".yaml";
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    public MigrationService(MigrationProperties migrationProperties) {
        this.migrationProperties = migrationProperties;
    }

    public static final class MigrationDiffResult {
        private final String changeLogAbsolutePath;
        private final String changeLogContent;
        private final boolean containsActionableChanges;

        public MigrationDiffResult(String changeLogAbsolutePath, String changeLogContent, boolean containsActionableChanges) {
            this.changeLogAbsolutePath = changeLogAbsolutePath;
            this.changeLogContent = changeLogContent;
            this.containsActionableChanges = containsActionableChanges;
        }
        public String getChangeLogAbsolutePath() { return changeLogAbsolutePath; }
        public String getChangeLogContent() { return changeLogContent; }
        public boolean containsActionableChanges() { return containsActionableChanges; }
    }

    // ============================================================
    // API Pública
    // ============================================================

    public void generateAndApply() throws Exception {
        MigrationDiffResult result = generateDiffChangeLog();
        applyChangeLog(result);
    }

    public MigrationDiffResult generateDiffChangeLog() throws Exception {
        Path outputDirectory = Paths.get(migrationProperties.getOutputDir()).toAbsolutePath().normalize();
        Files.createDirectories(outputDirectory);

        Path changeLogPath = outputDirectory.resolve(
                CHANGE_LOG_FILE_PREFIX + LocalDateTime.now().format(TIMESTAMP_FORMAT) + CHANGE_LOG_FILE_SUFFIX
        );

        // Liquibase + extensão Hibernate
        System.setProperty(LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE);

        String dialect = nullToEmpty(migrationProperties.getDialect()).trim();
        String referenceUrl = "hibernate:spring:" + buildEntityPackagesSegment(migrationProperties.getEntityPackage()) + "?dialect=" + dialect;

        CommandScope diff = new CommandScope("diffChangeLog");
        diff.addArgumentValue("referenceUrl", referenceUrl);
        diff.addArgumentValue("changeLogFile", changeLogPath.toString());
        diff.addArgumentValue("url", dataSourceUrl);
        putIfPresent(diff, "username", dataSourceUsername);
        putIfPresent(diff, "password", dataSourcePassword);

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        ResourceAccessor classpathAccessor = new ClassLoaderResourceAccessor(contextClassLoader);

        Map<String, Object> scope = Map.of(
                Scope.Attr.resourceAccessor.name(), classpathAccessor,
                Scope.Attr.classLoader.name(), contextClassLoader,
                LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE
        );

        Scope.child(scope, () -> { diff.execute(); return null; });

        String generatedYaml = Files.readString(changeLogPath, StandardCharsets.UTF_8);

        boolean hasChangeSets = generatedYaml.contains("\n- changeSet:");
        if (!hasChangeSets && migrationProperties.isSkipWhenEmpty()) {
            Files.deleteIfExists(changeLogPath);
            LOGGER.info("Nenhuma diferença detectada. Nada a aplicar.");
            return new MigrationDiffResult(null, "", false);
        }

        String finalYaml = generatedYaml;

        // Ajuste específico para SQLite (e genericamente útil): nomear constraints ausentes
        if (migrationProperties.isAutoNameConstraints()) {
            finalYaml = postProcessYamlForConstraints(finalYaml);
            if (!Objects.equals(generatedYaml, finalYaml)) {
                Files.writeString(changeLogPath, finalYaml, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);
                LOGGER.info("YAML ajustado: constraints sem nome foram nomeadas automaticamente.");
            }
        }

        if (migrationProperties.isLogDiffContent()) {
            LOGGER.info("YAML gerado:\n{}", finalYaml);
        }

        LOGGER.info("ChangeLog gerado em {}", changeLogPath);
        return new MigrationDiffResult(changeLogPath.toString(), finalYaml, hasChangeSets);
    }

    public void applyChangeLog(MigrationDiffResult result) throws Exception {
        if (result == null || !result.containsActionableChanges() || result.getChangeLogAbsolutePath() == null) {
            LOGGER.info("Sem mudanças — aplicação ignorada.");
            return;
        }
        applyChangeLog(result.getChangeLogAbsolutePath());
    }

    public void applyChangeLog(String changeLogAbsolutePath) throws Exception {
        if (changeLogAbsolutePath == null || changeLogAbsolutePath.isBlank()) {
            LOGGER.info("applyChangeLog chamado sem arquivo. Ignorando.");
            return;
        }

        Path path = Paths.get(changeLogAbsolutePath).toAbsolutePath().normalize();
        if (!Files.exists(path)) {
            throw new IOException("Arquivo não encontrado: " + path);
        }

        System.setProperty(LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE);

        CommandScope update = new CommandScope("update");
        update.addArgumentValue("changelogFile", path.getFileName().toString()); // apenas o nome: a pasta vem no DirectoryResourceAccessor
        update.addArgumentValue("url", dataSourceUrl);
        putIfPresent(update, "username", dataSourceUsername);
        putIfPresent(update, "password", dataSourcePassword);

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        ResourceAccessor classpathAccessor = new ClassLoaderResourceAccessor(contextClassLoader);
        ResourceAccessor directoryAccessor = new DirectoryResourceAccessor(path.getParent());
        ResourceAccessor compositeAccessor = new CompositeResourceAccessor(directoryAccessor, classpathAccessor);

        Map<String, Object> scope = Map.of(
                Scope.Attr.resourceAccessor.name(), compositeAccessor,
                Scope.Attr.classLoader.name(), contextClassLoader,
                LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE
        );

        Scope.child(scope, () -> { update.execute(); return null; });

        LOGGER.info("ChangeLog aplicado: {}", path);
    }

    // ============================================================
    // Auxiliares (Clean Code / SRP)
    // ============================================================

    /** Monta o segmento de pacotes do "hibernate:spring:pkg1,pkg2". */
    private static String buildEntityPackagesSegment(String configuredPackages) {
        String source = (configuredPackages == null || configuredPackages.isBlank())
                ? "com.exemplo.domain"
                : configuredPackages;

        String[] split = source.split("[,;]");
        List<String> cleaned = new ArrayList<>();
        for (String s : split) {
            String pkg = s.trim();
            if (!pkg.isEmpty()) cleaned.add(pkg);
        }
        return String.join(",", cleaned);
    }

    private static void putIfPresent(CommandScope command, String argumentName, String value) {
        if (value != null && !value.isBlank()) {
            command.addArgumentValue(argumentName, value);
        }
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    // ============================================================
    // Pós-processamento do YAML (SQLite-friendly)
    // ============================================================

    /**
     * Varre o YAML e injeta 'constraintName' quando ausente nos blocos:
     *  - addForeignKeyConstraint
     *  - dropForeignKeyConstraint
     *
     * Regra de nome:
     *   fk_<tabela_base>_<colunas_base>__<tabela_ref>   (máx. 60 chars, sanetizado)
     *   Para DROP quando não há referencedTableName, cai em: fk_<tabela_base>_<colunas_base>
     */
    private static String postProcessYamlForConstraints(String yaml) {
        List<String> lines = new ArrayList<>(Arrays.asList(yaml.split("\n", -1)));
        boolean anyChange = false;

        for (int i = 0; i < lines.size(); i++) {
            String current = lines.get(i);

            if (current.contains("- addForeignKeyConstraint:")) {
                int start = i;
                int end = findBlockEnd(lines, i + 1);

                Map<String, String> fields = readKeyValues(lines, start + 1, end);
                if (!fields.containsKey("constraintName")) {
                    String baseTable = fields.getOrDefault("baseTableName", "table");
                    String baseColumns = fields.getOrDefault("baseColumnNames", "col");
                    String referencedTable = fields.getOrDefault("referencedTableName", "ref");

                    String autoName = buildForeignKeyName(baseTable, baseColumns, referencedTable);

                    String indent = leadingSpacesOf(lines.get(start + 1));
                    lines.add(start + 1, indent + "constraintName: " + autoName);
                    anyChange = true;
                }
                i = end;
            }
            else if (current.contains("- dropForeignKeyConstraint:")) {
                int start = i;
                int end = findBlockEnd(lines, i + 1);

                Map<String, String> fields = readKeyValues(lines, start + 1, end);
                if (!fields.containsKey("constraintName")) {
                    String baseTable = fields.getOrDefault("baseTableName", "table");
                    String baseColumns = fields.getOrDefault("baseColumnNames", "col");
                    String referencedTable = fields.get("referencedTableName");

                    String autoName = (referencedTable == null || referencedTable.isBlank())
                            ? buildForeignKeyName(baseTable, baseColumns, null)
                            : buildForeignKeyName(baseTable, baseColumns, referencedTable);

                    String indent = leadingSpacesOf(lines.get(start + 1));
                    lines.add(start + 1, indent + "constraintName: " + autoName);
                    anyChange = true;
                }
                i = end;
            }
        }
        return anyChange ? String.join("\n", lines) : yaml;
    }

    /** Lê pares chave:valor simples no nível atual. */
    private static Map<String, String> readKeyValues(List<String> lines, int startInclusive, int endExclusive) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = startInclusive; i < endExclusive && i < lines.size(); i++) {
            String raw = lines.get(i);
            if (raw == null) continue;
            String trimmed = raw.trim();

            // Para nosso uso: linhas 'key: value'
            int idx = trimmed.indexOf(':');
            if (idx > 0) {
                String key = trimmed.substring(0, idx).trim();
                String val = trimmed.substring(idx + 1).trim();
                val = stripQuotes(val);
                if (!key.isEmpty()) {
                    map.put(key, val);
                }
            }
        }
        return map;
    }

    /** Encontra o fim lógico do bloco (heurística baseada em linhas que iniciam um novo item/changeset). */
    private static int findBlockEnd(List<String> lines, int fromIndex) {
        for (int i = fromIndex; i < lines.size(); i++) {
            String s = lines.get(i);
            if (s == null) continue;
            String trimmed = s.trim();

            if (trimmed.startsWith("- ")) { // próximo item no mesmo nível
                return i;
            }
            if (trimmed.equals("- changeSet:") || trimmed.startsWith("changeSet:")) {
                return i;
            }
        }
        return lines.size();
    }

    private static String stripQuotes(String value) {
        if (value == null) return null;
        if ((value.startsWith("'") && value.endsWith("'")) || (value.startsWith("\"") && value.endsWith("\""))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private static String leadingSpacesOf(String s) {
        if (s == null) return "";
        int i = 0;
        while (i < s.length() && Character.isWhitespace(s.charAt(i)) && s.charAt(i) != '\n' && s.charAt(i) != '\r') {
            i++;
        }
        return s.substring(0, i);
    }

    /** fk_<base>_<cols>__<ref> (ou fk_<base>_<cols> quando ref == null) — até 60 chars, sanetizado. */
    private static String buildForeignKeyName(String baseTable, String baseColumnsCsv, String referencedTableOrNull) {
        String base = slug(baseTable);
        String cols = slug(nullToEmpty(baseColumnsCsv).replace(",", "_"));
        String ref = (referencedTableOrNull == null) ? null : slug(referencedTableOrNull);

        String raw = (ref == null || ref.isBlank())
                ? String.format("fk_%s_%s", base, cols)
                : String.format("fk_%s_%s__%s", base, cols, ref);

        return raw.length() <= 60 ? raw : raw.substring(0, 60);
    }

    /** Mantém apenas [a-z0-9_], minúsculas, compacta múltiplos '_' e remove '_' do início. */
    private static String slug(String value) {
        if (value == null) return "v";
        String lower = value.toLowerCase(Locale.ROOT);
        StringBuilder builder = new StringBuilder(lower.length());
        for (int i = 0; i < lower.length(); i++) {
            char c = lower.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') builder.append(c);
            else builder.append('_');
        }
        String normalized = builder.toString().replaceAll("_+", "_");
        if (normalized.startsWith("_")) normalized = normalized.substring(1);
        return normalized.isEmpty() ? "v" : normalized;
    }
}
