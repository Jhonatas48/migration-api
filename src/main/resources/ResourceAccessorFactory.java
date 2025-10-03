
package com.github.jhonatas48.migrationapi.support;

import liquibase.Scope;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.CompositeResourceAccessor;
import liquibase.resource.DirectoryResourceAccessor;
import liquibase.resource.ResourceAccessor;

import java.nio.file.Path;
import java.util.Map;

public class ResourceAccessorFactory {

    private static final String LIQUIBASE_PLUGIN_PACKAGES_PROP = "liquibase.plugin.packages";
    private static final String HIBERNATE_PLUGIN_PACKAGE = "liquibase.ext.hibernate";

    public ResourceAccessor forFileInDirectory(Path directory) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        ResourceAccessor classpathAccessor = new ClassLoaderResourceAccessor(contextClassLoader);
        ResourceAccessor directoryAccessor = new DirectoryResourceAccessor(directory);
        return new CompositeResourceAccessor(directoryAccessor, classpathAccessor);
    }

    public Map<String, Object> buildScope(ResourceAccessor accessor) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        return Map.of(
                Scope.Attr.resourceAccessor.name(), accessor,
                Scope.Attr.classLoader.name(), contextClassLoader,
                LIQUIBASE_PLUGIN_PACKAGES_PROP, HIBERNATE_PLUGIN_PACKAGE
        );
    }
}
