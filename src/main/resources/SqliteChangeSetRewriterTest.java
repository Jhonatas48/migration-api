package com.github.jhonatas48.migrationapi.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Testa a reescrita "SQLite-friendly" de changes quando operações não são suportadas diretamente.
 * Assumimos que a classe SqliteChangeSetRewriter expõe um método estático:
 *   String rewrite(String yaml)
 */
public class SqliteChangeSetRewriterTest {

    @Test
    @DisplayName("Deve manter semanticamente o YAML e marcar operações sensíveis para recriação de tabela")
    void shouldRewriteToBeSqliteFriendly() {
        String yaml = ""
            + "databaseChangeLog:\n"
            + "- changeSet:\n"
            + "    id: 10\n"
            + "    author: test\n"
            + "    changes:\n"
            + "    - addForeignKeyConstraint:\n"
            + "        baseTableName: child\n"
            + "        baseColumnNames: parent_id\n"
            + "        referencedTableName: parent\n";

        String rewritten = SqliteChangeSetRewriter.rewrite(yaml);

        // O teste é conservador: apenas verifica que um marcador foi inserido,
        // indicando que a operação requer recriação (contrato do rewriter).
        assertThat(rewritten).contains("# SQLITE_REWRITE: requires table recreation");
    }
}
