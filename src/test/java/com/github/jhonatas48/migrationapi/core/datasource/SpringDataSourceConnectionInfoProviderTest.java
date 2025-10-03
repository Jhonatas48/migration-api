package com.github.jhonatas48.migrationapi.core.datasource;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.mock.env.MockEnvironment;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.*;

class SpringDataSourceConnectionInfoProviderTest {

    @Test
    void shouldResolveAllPropertiesFromSpringEnvironment() {
        // Arrange (segue usando MockEnvironment, mas agora convertemos para DataSourceProperties)
        MockEnvironment env = new MockEnvironment()
                .withProperty("spring.datasource.url", "jdbc:sqlite:./target/testdb.sqlite")
                .withProperty("spring.datasource.username", "user")
                .withProperty("spring.datasource.password", "pass");

        DataSourceProperties props = new DataSourceProperties();
        props.setUrl(env.getProperty("spring.datasource.url"));
        props.setUsername(env.getProperty("spring.datasource.username"));
        props.setPassword(env.getProperty("spring.datasource.password"));

        DataSource dataSource = null; // não precisamos dele para este caso

        SpringDataSourceConnectionInfoProvider provider =
                new SpringDataSourceConnectionInfoProvider(props, dataSource);

        // Act
        DataSourceConnectionInfo info = provider.resolve();

        // Assert
        assertThat(info.getJdbcUrl()).isEqualTo("jdbc:sqlite:./target/testdb.sqlite");
        assertThat(info.getUsername()).isEqualTo("user");
        assertThat(info.getPassword()).isEqualTo("pass");
    }

    @Test
    void shouldThrowWhenUrlIsMissing() {
        // Arrange: sem URL nas propriedades e sem DataSource (força a exceção)
        DataSourceProperties props = new DataSourceProperties();
        DataSource dataSource = null;

        SpringDataSourceConnectionInfoProvider provider =
                new SpringDataSourceConnectionInfoProvider(props, dataSource);

        // Act + Assert
        assertThatThrownBy(provider::resolve)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("spring.datasource.url"); // bate com a mensagem da classe
    }
}
