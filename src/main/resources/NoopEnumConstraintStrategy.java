
package com.github.jhonatas48.migrationapi.core;

public class NoopEnumConstraintStrategy implements EnumConstraintStrategy {
    @Override
    public String process(String yaml) {
        return yaml;
    }
}
