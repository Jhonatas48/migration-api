
package com.github.jhonatas48.migrationapi.core;

import com.github.jhonatas48.migrationapi.support.ResourceAccessorFactory;
import liquibase.Scope;
import liquibase.command.CommandScope;
import liquibase.resource.ResourceAccessor;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class ChangeLogApplier {

    private final ResourceAccessorFactory resourceAccessorFactory;

    public ChangeLogApplier(ResourceAccessorFactory resourceAccessorFactory) {
        this.resourceAccessorFactory = resourceAccessorFactory;
    }

    public void applyChangeLog(String changeLogAbsolutePath) throws Exception {
        Path path = Paths.get(changeLogAbsolutePath).toAbsolutePath().normalize();

        CommandScope update = new CommandScope("update");
        update.addArgumentValue("changelogFile", path.getFileName().toString());
        update.addArgumentValue("url", System.getProperty("spring.datasource.url"));

        String username = System.getProperty("spring.datasource.username");
        String password = System.getProperty("spring.datasource.password");
        if (username != null && !username.isBlank()) update.addArgumentValue("username", username);
        if (password != null && !password.isBlank()) update.addArgumentValue("password", password);

        ResourceAccessor accessor = resourceAccessorFactory.forFileInDirectory(path.getParent());
        Map<String, Object> scope = resourceAccessorFactory.buildScope(accessor);
        Scope.child(scope, () -> { update.execute(); return null; });
    }
}
