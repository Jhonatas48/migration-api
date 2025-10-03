package com.github.jhonatas48.migrationapi.strategy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Por padrão não altera o YAML (opt-in para checks de enum).
 */
public class NoopEnumConstraintStrategyTest {

    @Test
    @DisplayName("Não deve alterar o YAML original")
    void shouldNotAlterYaml() {
        String originalYaml = "databaseChangeLog:\n- changeSet:\n  id: 1\n  author: a\n";
        EnumConstraintStrategy strategy = new NoopEnumConstraintStrategy();
        String result = strategy.applyEnumChecks(originalYaml);
        assertThat(result).isEqualTo(originalYaml);
    }
}
