package com.github.jhonatas48.migrationapi.core.audit;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.Objects;

/**
 * Guarda um "hash" das transformações de FK já aplicadas,
 * para não repetir quando o YAML (ou seu trecho) for igual.
 */
public class MigrationAuditService {

    private final DataSource dataSource;

    public MigrationAuditService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void ensureTable() throws SQLException {
        try (Connection c = dataSource.getConnection();
             Statement st = c.createStatement()) {
            st.execute("""
                    CREATE TABLE IF NOT EXISTS MIGRATION_API_AUDIT (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        hash VARCHAR(128) NOT NULL UNIQUE,
                        description TEXT,
                        applied_at TEXT NOT NULL
                    )
                    """);
        }
    }

    public boolean wasAlreadyApplied(String hash) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT 1 FROM MIGRATION_API_AUDIT WHERE hash = ?")) {
            ps.setString(1, hash);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    public void recordApplied(String hash, String description) throws SQLException {
        Objects.requireNonNull(hash, "hash");
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT OR IGNORE INTO MIGRATION_API_AUDIT(hash, description, applied_at) VALUES (?, ?, ?)")) {
            ps.setString(1, hash);
            ps.setString(2, description);
            ps.setString(3, Instant.now().toString());
            ps.executeUpdate();
        }
    }
}
