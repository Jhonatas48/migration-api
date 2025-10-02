package com.github.jhonatas48.migrationapi;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.migrations")
public class MigrationProperties {

    private String outputDir = "./src/main/resources/db/changelog/generated";
    private String dialect = "";
    private boolean runAtStartup = false;

    public String getOutputDir() { return outputDir; }
    public void setOutputDir(String outputDir) { this.outputDir = outputDir; }

    public String getDialect() { return dialect; }
    public void setDialect(String dialect) { this.dialect = dialect; }

    public boolean isRunAtStartup() { return runAtStartup; }
    public void setRunAtStartup(boolean runAtStartup) { this.runAtStartup = runAtStartup; }
}
