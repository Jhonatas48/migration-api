package com.github.jhonatas48.migrationapi;

import com.github.jhonatas48.migrationapi.core.ChangeLogApplier;
import com.github.jhonatas48.migrationapi.core.ChangeLogGenerator;
import com.github.jhonatas48.migrationapi.core.ChangeLogGenerator.GeneratedChangeLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orquestra a geração e aplicação de change logs do Liquibase.
 * Aplica princípios de Clean Code (nomes descritivos, SRP) e SOLID (Single Responsibility, Dependency Injection).
 */
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

    /**
     * DTO estável de retorno para consumidores do serviço.
     * Evita acoplamento com tipos internos do gerador.
     */
    public static final class MigrationDiffResult {
        private final String changeLogAbsolutePath;
        private final String changeLogContent;
        private final boolean containsActionableChanges;

        public MigrationDiffResult(String changeLogAbsolutePath,
                                   String changeLogContent,
                                   boolean containsActionableChanges) {
            this.changeLogAbsolutePath = changeLogAbsolutePath;
            this.changeLogContent = changeLogContent;
            this.containsActionableChanges = containsActionableChanges;
        }

        public String getChangeLogAbsolutePath() {
            return changeLogAbsolutePath;
        }

        public String getChangeLogContent() {
            return changeLogContent;
        }

        public boolean containsActionableChanges() {
            return containsActionableChanges;
        }
    }

    /**
     * Gera e aplica o change log numa única chamada, se houver mudanças.
     */
    public void generateAndApply() throws Exception {
        MigrationDiffResult diffResult = generateDiffChangeLog();
        applyChangeLog(diffResult);
    }

    /**
     * Gera o change log a partir do estado atual do banco comparado ao modelo Hibernate.
     * Converte o resultado interno do gerador para o DTO público do serviço.
     */
    public MigrationDiffResult generateDiffChangeLog() throws Exception {
        GeneratedChangeLog generated = changeLogGenerator.generateDiffChangeLog();

        String absolutePath = generated.getAbsolutePath() != null
                ? generated.getAbsolutePath().toString()
                : null;

        return new MigrationDiffResult(
                absolutePath,
                generated.getYamlContent(),
                generated.isContainsChangeSets()
        );
    }

    /**
     * Aplica o change log quando existe algo a ser executado.
     */
    public void applyChangeLog(MigrationDiffResult diffResult) throws Exception {
        if (diffResult == null
                || !diffResult.containsActionableChanges()
                || diffResult.getChangeLogAbsolutePath() == null
                || diffResult.getChangeLogAbsolutePath().isBlank()) {
            LOGGER.info("Sem mudanças — aplicação ignorada.");
            return;
        }

        changeLogApplier.applyChangeLog(diffResult.getChangeLogAbsolutePath());
    }
}
