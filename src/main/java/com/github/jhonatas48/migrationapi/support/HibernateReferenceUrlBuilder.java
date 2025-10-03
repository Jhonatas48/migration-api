
package com.github.jhonatas48.migrationapi.support;

import com.github.jhonatas48.migrationapi.MigrationProperties;

import java.util.ArrayList;
import java.util.List;

public class HibernateReferenceUrlBuilder {

    public String buildReferenceUrl(MigrationProperties properties) {
        String dialect = properties.getDialect() == null ? "" : properties.getDialect().trim();
        String packages = buildEntityPackagesSegment(properties.getEntityPackage());
        return "hibernate:spring:" + packages + "?dialect=" + dialect;
    }

    private String buildEntityPackagesSegment(String configuredPackages) {
        String source = (configuredPackages == null || configuredPackages.isBlank())
                ? "com.exemplo.domain"
                : configuredPackages;

        String[] split = source.split("[,;]");
        List<String> cleaned = new ArrayList<>();
        for (String s : split) {
            String pkg = s.trim();
            if (!pkg.isEmpty()) cleaned.add(pkg);
        }
        return String.join(",", cleaned);
    }
}
