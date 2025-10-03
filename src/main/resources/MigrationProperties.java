
package com.github.jhonatas48.migrationapi;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "migrationapi")
public class MigrationProperties {

    private boolean enabled = true;
    private boolean generateAndApply = true;
    private String outputDir = "./src/main/resources/db/changelog/generated";
    private String dialect = "";
    private String entityPackage = "com.exemplo.domain";
    private boolean skipWhenEmpty = true;
    private boolean logDiffContent = false;
    private boolean autoNameConstraints = true;
    private boolean sqliteRewriteEnabled = true;
    private boolean enumCheckEnabled = false;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public boolean isGenerateAndApply() { return generateAndApply; }
    public void setGenerateAndApply(boolean generateAndApply) { this.generateAndApply = generateAndApply; }
    public String getOutputDir() { return outputDir; }
    public void setOutputDir(String outputDir) { this.outputDir = outputDir; }
    public String getDialect() { return dialect; }
    public void setDialect(String dialect) { this.dialect = dialect; }
    public String getEntityPackage() { return entityPackage; }
    public void setEntityPackage(String entityPackage) { this.entityPackage = entityPackage; }
    public boolean isSkipWhenEmpty() { return skipWhenEmpty; }
    public void setSkipWhenEmpty(boolean skipWhenEmpty) { this.skipWhenEmpty = skipWhenEmpty; }
    public boolean isLogDiffContent() { return logDiffContent; }
    public void setLogDiffContent(boolean logDiffContent) { this.logDiffContent = logDiffContent; }
    public boolean isAutoNameConstraints() { return autoNameConstraints; }
    public void setAutoNameConstraints(boolean autoNameConstraints) { this.autoNameConstraints = autoNameConstraints; }
    public boolean isSqliteRewriteEnabled() { return sqliteRewriteEnabled; }
    public void setSqliteRewriteEnabled(boolean sqliteRewriteEnabled) { this.sqliteRewriteEnabled = sqliteRewriteEnabled; }
    public boolean isEnumCheckEnabled() { return enumCheckEnabled; }
    public void setEnumCheckEnabled(boolean enumCheckEnabled) { this.enumCheckEnabled = enumCheckEnabled; }
}
