package com.github.jhonatas48.migrationapi;

import com.github.jhonatas48.migrationapi.core.ChangeLogApplier;
import com.github.jhonatas48.migrationapi.core.ChangeLogGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Testa a orquestração do MigrationService usando dublês.
 * Os detalhes de Liquibase não são exercitados aqui; foco na lógica de fluxo.
 */
@ExtendWith(MockitoExtension.class)
public class MigrationServiceOrchestrationTest {

    @Mock
    private ChangeLogGenerator changeLogGenerator;

    @Mock
    private ChangeLogApplier changeLogApplier;

    @Mock
    private MigrationProperties migrationProperties;

    @Captor
    private ArgumentCaptor<String> pathCaptor;

    @InjectMocks
    private MigrationService migrationService; // supondo construtor que recebe (props, generator, applier)

    @Test
    @DisplayName("generateAndApply chama generator e, havendo mudanças, chama applier")
    void shouldGenerateAndApplyWhenThereAreChanges() throws Exception {
        when(migrationProperties.isSkipWhenEmpty()).thenReturn(true);

        ChangeLogGenerator.MigrationDiffResult fake = new ChangeLogGenerator.MigrationDiffResult(
                "/tmp/diff.yaml",
                "databaseChangeLog:\n- changeSet:\n",
                true
        );
        when(changeLogGenerator.generateDiffChangeLog()).thenReturn(fake);

        migrationService.generateAndApply();

        verify(changeLogGenerator, times(1)).generateDiffChangeLog();
        verify(changeLogApplier, times(1)).applyChangeLog(pathCaptor.capture());
        assertThat(pathCaptor.getValue()).isEqualTo("/tmp/diff.yaml");
    }

    @Test
    @DisplayName("generateAndApply não chama applier quando não há mudanças")
    void shouldNotApplyWhenThereAreNoChanges() throws Exception {
        when(migrationProperties.isSkipWhenEmpty()).thenReturn(true);

        ChangeLogGenerator.MigrationDiffResult fake = new ChangeLogGenerator.MigrationDiffResult(
                null,
                "",
                false
        );
        when(changeLogGenerator.generateDiffChangeLog()).thenReturn(fake);

        migrationService.generateAndApply();

        verify(changeLogGenerator, times(1)).generateDiffChangeLog();
        verifyNoInteractions(changeLogApplier);
    }
}
