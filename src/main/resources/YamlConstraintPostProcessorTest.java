package com.github.jhonatas48.migrationapi.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Testes para o pós-processador que nomeia constraints ausentes em YAML.
 * Estes testes assumem que a classe YamlConstraintPostProcessor expõe um método estático:
 *   String postProcessYamlForConstraints(String yaml)
 */
public class YamlConstraintPostProcessorTest {

    @Test
    @DisplayName("Deve nomear constraint ausente em addForeignKeyConstraint")
    void shouldNameMissingConstraintForAddForeignKey() {
        String yaml = ""
            + "databaseChangeLog:\n"
            + "- changeSet:\n"
            + "    id: 1\n"
            + "    author: test\n"
            + "    changes:\n"
            + "    - addForeignKeyConstraint:\n"
            + "        baseTableName: order_item\n"
            + "        baseColumnNames: product_id\n"
            + "        referencedTableName: product\n";

        String processed = YamlConstraintPostProcessor.postProcessYamlForConstraints(yaml);

        assertThat(processed).contains("constraintName: fk_order_item_product_id__product");
    }

    @Test
    @DisplayName("Deve nomear constraint ausente em dropForeignKeyConstraint quando constraintName está vazio")
    void shouldNameMissingConstraintForDropForeignKey() {
        String yaml = ""
            + "databaseChangeLog:\n"
            + "- changeSet:\n"
            + "    id: 2\n"
            + "    author: test\n"
            + "    changes:\n"
            + "    - dropForeignKeyConstraint:\n"
            + "        baseTableName: revision_punishment\n"
            + "        constraintName: ''\n";

        String processed = YamlConstraintPostProcessor.postProcessYamlForConstraints(yaml);

        // Como não temos as colunas, o algoritmo usa defaults, mas garante que um nome não-vazio estará presente
        assertThat(processed).doesNotContain("constraintName: ''");
        assertThat(processed).contains("constraintName: fk_revision_punishment_col");
    }
}
