package com.github.jhonatas48.migrationapi;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.TestPropertySource;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Testa a integração leve do AutoMigrationRunner dentro do contexto Spring.
 * Como não queremos execução real de Liquibase, fazemos mock de MigrationService.
 */
@SpringBootTest
@TestPropertySource(properties = {
        "migrationapi.enabled=true",
        "migrationapi.generate-and-apply=false" // apenas gerar no runner
})
public class AutoMigrationRunnerSpringTest {

    @Autowired
    private AutoMigrationRunner autoMigrationRunner;

    @MockBean
    private MigrationService migrationService;

    @Test
    @DisplayName("AutoMigrationRunner chama somente geração quando generate-and-apply=false")
    void shouldOnlyGenerateWhenFlagIsFalse() throws Exception {
        autoMigrationRunner.run(null);
        verify(migrationService, times(1)).generateDiffChangeLog();
        Mockito.verifyNoMoreInteractions(migrationService);
    }
}
