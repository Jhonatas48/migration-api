
package com.github.jhonatas48.migrationapi;

import com.github.jhonatas48.migrationapi.core.ChangeLogApplier;
import com.github.jhonatas48.migrationapi.core.ChangeLogGenerator;
import com.github.jhonatas48.migrationapi.core.EnumConstraintStrategy;
import com.github.jhonatas48.migrationapi.core.NoopEnumConstraintStrategy;
import com.github.jhonatas48.migrationapi.core.SqliteChangeSetRewriter;
import com.github.jhonatas48.migrationapi.core.YamlConstraintPostProcessor;
import com.github.jhonatas48.migrationapi.support.HibernateReferenceUrlBuilder;
import com.github.jhonatas48.migrationapi.support.ResourceAccessorFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties(MigrationProperties.class)
public class MigrationAutoConfiguration {

    @Bean
    public MigrationService migrationService(MigrationProperties properties,
                                             ChangeLogGenerator changeLogGenerator,
                                             ChangeLogApplier changeLogApplier) {
        return new MigrationService(properties, changeLogGenerator, changeLogApplier);
    }

    @Bean
    public AutoMigrationRunner autoMigrationRunner(MigrationProperties properties,
                                                   MigrationService migrationService) {
        return new AutoMigrationRunner(properties, migrationService);
    }

    @Bean
    public ChangeLogGenerator changeLogGenerator(MigrationProperties properties,
                                                 HibernateReferenceUrlBuilder referenceUrlBuilder,
                                                 YamlConstraintPostProcessor yamlConstraintPostProcessor,
                                                 SqliteChangeSetRewriter sqliteChangeSetRewriter) {
        EnumConstraintStrategy enumStrategy = new NoopEnumConstraintStrategy();
        return new ChangeLogGenerator(properties, referenceUrlBuilder, yamlConstraintPostProcessor, sqliteChangeSetRewriter, enumStrategy);
    }

    @Bean
    public ChangeLogApplier changeLogApplier(ResourceAccessorFactory resourceAccessorFactory) {
        return new ChangeLogApplier(resourceAccessorFactory);
    }

    @Bean
    public HibernateReferenceUrlBuilder referenceUrlBuilder() {
        return new HibernateReferenceUrlBuilder();
    }

    @Bean
    public YamlConstraintPostProcessor yamlConstraintPostProcessor(MigrationProperties properties) {
        return new YamlConstraintPostProcessor(properties);
    }

    @Bean
    public SqliteChangeSetRewriter sqliteChangeSetRewriter(MigrationProperties properties) {
        return new SqliteChangeSetRewriter(properties);
    }

    @Bean
    public ResourceAccessorFactory resourceAccessorFactory() {
        return new ResourceAccessorFactory();
    }
}
