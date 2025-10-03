package com.github.jhonatas48.migrationapi;

import com.github.jhonatas48.migrationapi.core.ChangeLogApplier;
import com.github.jhonatas48.migrationapi.core.ChangeLogGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationService.class);

    private final MigrationProperties migrationProperties;
    private final ChangeLogGenerator changeLogGenerator;
    private final ChangeLogApplier changeLogApplier;

    public MigrationService(MigrationProperties migrationProperties,
                            ChangeLogGenerator changeLogGenerator,
                            ChangeLogApplier changeLogApplier) {
        this.migrationProperties = migrationProperties;
        this.changeLogGenerator = changeLogGenerator;
        this.changeLogApplier = changeLogApplier;
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

    public void generateAndApply() throws Exception {
        MigrationDiffResult result = generateDiffChangeLog();
        applyChangeLog(result);
    }

    public MigrationDiffResult generateDiffChangeLog() throws Exception {
        ChangeLogGenerator.GeneratedChangeLog gen = changeLogGenerator.generateDiffChangeLog();
        return new MigrationDiffResult(gen.getAbsolutePath(), gen.getContent(), gen.hasActionableChanges());
    }

    public void applyChangeLog(MigrationDiffResult result) throws Exception {
        if (result == null || !result.containsActionableChanges() || result.getChangeLogAbsolutePath() == null) {
            LOGGER.info("Sem mudanças — aplicação ignorada.");
            return;
        }
        changeLogApplier.applyChangeLog(result.getChangeLogAbsolutePath());
    }
}
