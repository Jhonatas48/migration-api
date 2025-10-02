package com.github.jhonatas48.migrationapi;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "migrationapi")
public class MigrationProperties {

    /** Ativa/desativa a execução automática no startup (via ApplicationRunner). */
    private boolean enabled = true;

    /** Quando true: gera e aplica de uma vez; quando false: apenas gera. */
    private boolean generateAndApply = true;

    /** Diretório de saída do changelog gerado. */
    private String outputDir = "./src/main/resources/db/changelog/generated";

    /** Dialect do Hibernate usado no "hibernate:spring:..." (ex.: org.hibernate.community.dialect.SQLiteDialect). */
    private String dialect = "";

    /**
     * Um ou mais pacotes de entidades, separados por vírgula ou ponto e vírgula.
     * Ex.: "com.meuapp.model, com.meuapp.outro.pacote"
     */
    private String entityPackage = "com.exemplo.domain";

    /** Se não houver changesets, ignora a aplicação. */
    private boolean skipWhenEmpty = true;

    /** Faz log do YAML gerado (cuidado em produção). */
    private boolean logDiffContent = false;

    /** Nomeia automaticamente constraints sem nome (FKs) no YAML. */
    private boolean autoNameConstraints = true;

    // Getters/Setters

    public boolean isEnabled() {
        return enabled;
    }
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isGenerateAndApply() {
        return generateAndApply;
    }
    public void setGenerateAndApply(boolean generateAndApply) {
        this.generateAndApply = generateAndApply;
    }

    public String getOutputDir() {
        return outputDir;
    }
    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }

    public String getDialect() {
        return dialect;
    }
    public void setDialect(String dialect) {
        this.dialect = dialect;
    }

    public String getEntityPackage() {
        return entityPackage;
    }
    public void setEntityPackage(String entityPackage) {
        this.entityPackage = entityPackage;
    }

    public boolean isSkipWhenEmpty() {
        return skipWhenEmpty;
    }
    public void setSkipWhenEmpty(boolean skipWhenEmpty) {
        this.skipWhenEmpty = skipWhenEmpty;
    }

    public boolean isLogDiffContent() {
        return logDiffContent;
    }
    public void setLogDiffContent(boolean logDiffContent) {
        this.logDiffContent = logDiffContent;
    }

    public boolean isAutoNameConstraints() {
        return autoNameConstraints;
    }
    public void setAutoNameConstraints(boolean autoNameConstraints) {
        this.autoNameConstraints = autoNameConstraints;
    }
}
