package com.github.jhonatas48.migrationapi.support;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Testes da construção da URL de referência do Hibernate para o Liquibase.
 */
public class HibernateReferenceUrlBuilderTest {

    @Test
    @DisplayName("Deve montar URL com múltiplos pacotes e dialect definido")
    void shouldBuildUrlWithMultiplePackagesAndDialect() {
        String url = HibernateReferenceUrlBuilder.build(
                "org.hibernate.community.dialect.SQLiteDialect",
                "io.github.app.domain, io.github.app.more"
        );
        assertThat(url).isEqualTo("hibernate:spring:io.github.app.domain,io.github.app.more?dialect=org.hibernate.community.dialect.SQLiteDialect");
    }

    @Test
    @DisplayName("Deve limpar espaços e separadores diversos")
    void shouldNormalizeSeparators() {
        String url = HibernateReferenceUrlBuilder.build(
                "dialectX",
                "  a.b.c ;  x.y.z  , m.n  "
        );
        assertThat(url).isEqualTo("hibernate:spring:a.b.c,x.y.z,m.n?dialect=dialectX");
    }
}
