package com.github.jhonatas48.migrationapi.core;

import com.github.jhonatas48.migrationapi.MigrationProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class YamlConstraintPostProcessorTest {

    @Test
    void shouldInjectConstraintNameForAddForeignKeyWhenMissing() {
        MigrationProperties props = new MigrationProperties();
        props.setAutoNameConstraints(true);

        YamlConstraintPostProcessor processor = new YamlConstraintPostProcessor(props);

        String yaml = """
            databaseChangeLog:
            - changeSet:
                id: 1
                author: test
                changes:
                - addForeignKeyConstraint:
                    baseTableName: rating
                    baseColumnNames: user_id
                    referencedTableName: user
            """;

        String out = processor.process(yaml);

        assertThat(out).contains("constraintName: fk_rating_user_id__user");
    }

    @Test
    void shouldInjectConstraintNameForDropForeignKeyWhenMissing() {
        MigrationProperties props = new MigrationProperties();
        props.setAutoNameConstraints(true);

        YamlConstraintPostProcessor processor = new YamlConstraintPostProcessor(props);

        String yaml = """
            databaseChangeLog:
            - changeSet:
                id: 1
                author: test
                changes:
                - dropForeignKeyConstraint:
                    baseTableName: revision_punishment
                    baseColumnNames: punishment_id
            """;

        String out = processor.process(yaml);

        assertThat(out).contains("constraintName: fk_revision_punishment_punishment_id");
    }

    @Test
    void shouldNotChangeYamlWhenConstraintNameAlreadyPresent() {
        MigrationProperties props = new MigrationProperties();
        props.setAutoNameConstraints(true);

        YamlConstraintPostProcessor processor = new YamlConstraintPostProcessor(props);

        String yaml = """
            databaseChangeLog:
            - changeSet:
                id: 1
                author: test
                changes:
                - addForeignKeyConstraint:
                    baseTableName: rating
                    baseColumnNames: user_id
                    referencedTableName: user
                    constraintName: fk_custom_name
            """;

        String out = processor.process(yaml);

        assertThat(out).contains("constraintName: fk_custom_name");
        assertThat(out).doesNotContain("fk_rating_user_id__user\n");
    }

    @Test
    void shouldNotChangeYamlWhenAutoNameConstraintsIsDisabled() {
        MigrationProperties props = new MigrationProperties();
        props.setAutoNameConstraints(false);

        YamlConstraintPostProcessor processor = new YamlConstraintPostProcessor(props);

        String yaml = """
            databaseChangeLog:
            - changeSet:
                id: 1
                author: test
                changes:
                - addForeignKeyConstraint:
                    baseTableName: rating
                    baseColumnNames: user_id
                    referencedTableName: user
            """;

        String out = processor.process(yaml);

        assertThat(out).isEqualTo(yaml);
    }
}
