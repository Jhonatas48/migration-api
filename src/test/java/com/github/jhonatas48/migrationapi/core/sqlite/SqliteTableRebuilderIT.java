package com.github.jhonatas48.migrationapi.core.sqlite;

import com.github.jhonatas48.migrationapi.core.sqlite.SqliteTableRebuilder.ForeignKeySpec;
import org.junit.jupiter.api.*;
import org.sqlite.SQLiteConfig;

import javax.sql.DataSource;
import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class SqliteTableRebuilderIT {

    private static final String DB_PATH = "./target/allflows.sqlite";
    private DataSource dataSource;

    @BeforeEach
    void setup() throws Exception {
        // limpa arquivo anterior
        File f = new File(DB_PATH);
        if (f.exists()) assertThat(f.delete()).isTrue();

        this.dataSource = simpleDataSource("jdbc:sqlite:" + DB_PATH);

        try (Connection c = dataSource.getConnection()) {
            exec(c, "PRAGMA foreign_keys=ON");

            // ---------- Base 1: parent/child (simples) ----------
            exec(c, """
                CREATE TABLE "parent"(
                  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
                  "name" TEXT NOT NULL DEFAULT 'n/a',
                  CONSTRAINT uq_parent_name UNIQUE("name")
                )
                """);

            exec(c, """
                CREATE TABLE "child"(
                  "id" INTEGER PRIMARY KEY,
                  "parent_id" INTEGER NOT NULL,
                  "note" TEXT DEFAULT 'x',
                  CONSTRAINT fk_child_parent
                    FOREIGN KEY("parent_id") REFERENCES "parent"("id")
                    ON DELETE CASCADE MATCH SIMPLE
                )
                """);

            // índice parcial + trigger
            exec(c, """
                CREATE INDEX idx_child_note_nonempty
                ON "child"("note")
                WHERE "note" <> ''
                """);
            exec(c, """
                CREATE TRIGGER trg_child_bi
                BEFORE INSERT ON "child"
                FOR EACH ROW
                WHEN NEW."note" IS NULL
                BEGIN
                  SELECT RAISE(ABORT,'note required');
                END
                """);

            exec(c, "INSERT INTO parent(name) VALUES ('p1'),('p2')");
            exec(c, "INSERT INTO child(id,parent_id,note) VALUES (1,1,'a'),(2,2,'b')");

            // ---------- Base 2: FK composta ----------
            exec(c, """
                CREATE TABLE "parent_comp"(
                  "k1" INTEGER NOT NULL,
                  "k2" INTEGER NOT NULL,
                  "label" TEXT,
                  PRIMARY KEY("k1","k2")
                )
                """);
            exec(c, """
                CREATE TABLE "child_comp"(
                  "k1" INTEGER NOT NULL,
                  "k2" INTEGER NOT NULL,
                  "obs" TEXT DEFAULT 'ok',
                  CONSTRAINT fk_child_comp
                    FOREIGN KEY("k1","k2") REFERENCES "parent_comp"("k1","k2")
                    ON UPDATE NO ACTION ON DELETE RESTRICT
                )
                """);
            exec(c, "INSERT INTO parent_comp(k1,k2,label) VALUES (10,20,'a'),(11,22,'b')");
            exec(c, "INSERT INTO child_comp(k1,k2,obs) VALUES (10,20,'x'),(11,22,'y')");

            // ---------- Base 3: normalização de nomes ----------
            // Tabela criada como "Form_Developer" (para normalizar "FormDeveloper")
            exec(c, """
                CREATE TABLE "Form_Developer"(
                  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
                  "nick" TEXT
                )
                """);
            exec(c, """
                CREATE TABLE "uses_form"(
                  "id" INTEGER PRIMARY KEY,
                  "dev_id" INTEGER
                )
                """);
        }
    }

    @AfterEach
    void teardown() throws Exception {
        try (Connection c = dataSource.getConnection()) {
            exec(c, "PRAGMA wal_checkpoint(TRUNCATE)");
        } catch (Exception ignored) {}
        File f = new File(DB_PATH);
        if (f.exists()) assertThat(f.delete()).isTrue();
    }

    @Test
    void fluxo_basico_trocar_regra_FK_preservando_indices_triggers_e_dados() throws Exception {
        SqliteTableRebuilder rb = new SqliteTableRebuilder(dataSource);

        // Lê FK atual
        ForeignKeySpec fkAtual = readSingleFkSpec("child");
        assertThat(fkAtual.getOnDelete()).isEqualToIgnoringCase("CASCADE");
        String match = fkAtual.getMatch();
        assertThat(match == null ? "NONE" : match.toUpperCase())
                .isIn("SIMPLE", "NONE");


        // Trocar ON DELETE para RESTRICT, mantendo MATCH SIMPLE
        ForeignKeySpec drop = fkAtual;
        ForeignKeySpec add  = new ForeignKeySpec(
                fkAtual.getBaseColumnsCsv(), fkAtual.getReferencedTable(), fkAtual.getReferencedColumnsCsv(),
                "RESTRICT", fkAtual.getOnUpdate(), fkAtual.getMatch()
        );

        rb.rebuildTableApplyingForeignKeyChanges("child", List.of(add), List.of(drop));

        try (Connection c = dataSource.getConnection()) {
            // integridade ok
            assertThat(countForeignKeyViolations(c)).isZero();

            // índices/trigger preservados
            String idxSql = readSqlMaster(c, "index", "idx_child_note_nonempty");
            assertThat(idxSql).isNotBlank();
            String n = normSql(idxSql);

            // garante que é o índice certo e que é parcial (WHERE)
            assertThat(n).contains("create index idx_child_note_nonempty on child(note)");
            assertThat(n).contains("where note <> ''");


            // RESTRICT: agora apagar parent referenciado deve falhar
            assertThatThrownBy(() -> exec(c, "DELETE FROM parent WHERE id=1"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("FOREIGN KEY");

            // dados permanecem
            assertThat(queryLong(c, "SELECT COUNT(*) FROM child")).isEqualTo(2L);

            // sem resíduos
            assertThat(tableExists(c, "__tmp_child")).isFalse();
            assertThat(tableExists(c, "__bak_child")).isFalse();

            // FKs voltaram ON
            assertThat(getForeignKeysPragma(c)).isEqualTo(1);
        }
    }

    @Test
    void preserva_autoincrement_e_defaults_quando_nenhuma_fk_eh_mudada() throws Exception {
        SqliteTableRebuilder rb = new SqliteTableRebuilder(dataSource);

        rb.rebuildTableApplyingForeignKeyChanges("parent", List.of(), List.of());

        try (Connection c = dataSource.getConnection()) {
            // AUTOINCREMENT: id cresce sozinho
            exec(c, "INSERT INTO parent(name) VALUES ('p3')");
            long maxId = queryLong(c, "SELECT MAX(id) FROM parent");
            assertThat(maxId).isGreaterThan(2L);

            // DEFAULT preservado
            exec(c, "INSERT INTO parent(id) VALUES (NULL)"); // name -> 'n/a'
            String lastName = queryString(c, "SELECT name FROM parent ORDER BY id DESC LIMIT 1");
            assertThat(lastName).isEqualTo("n/a");

            assertThat(countForeignKeyViolations(c)).isZero();
            assertThat(getForeignKeysPragma(c)).isEqualTo(1);
        }
    }

    @Test
    void fk_composta_add_drop_e_onupdate_ondelete() throws Exception {
        SqliteTableRebuilder rb = new SqliteTableRebuilder(dataSource);

        // Lê FK composta existente
        ForeignKeySpec fkComp = readSingleFkSpec("child_comp");
        assertThat(fkComp.getBaseColumnsCsv()).isEqualTo("k1,k2");
        assertThat(fkComp.getReferencedColumnsCsv()).isEqualTo("k1,k2");
        assertThat(fkComp.getOnDelete()).isEqualToIgnoringCase("RESTRICT");

        // Trocar ON UPDATE para CASCADE e ON DELETE para NO ACTION
        ForeignKeySpec drop = fkComp;
        ForeignKeySpec add  = new ForeignKeySpec(
                "k1,k2", "parent_comp", "k1,k2", "NO ACTION", "CASCADE", fkComp.getMatch()
        );

        rb.rebuildTableApplyingForeignKeyChanges("child_comp", List.of(add), List.of(drop));

        try (Connection c = dataSource.getConnection()) {
            assertThat(countForeignKeyViolations(c)).isZero();

            // Testa efeito de ON UPDATE CASCADE: atualiza chave em parent_comp (10,20) -> (100,200)
            exec(c, "UPDATE parent_comp SET k1=100, k2=200 WHERE k1=10 AND k2=20");
            // deve ter atualizado child_comp também
            long cnt = queryLong(c, "SELECT COUNT(*) FROM child_comp WHERE k1=100 AND k2=200");
            assertThat(cnt).isEqualTo(1L);

            // ON DELETE NO ACTION: apagar parent ainda referenciado deve falhar
            assertThatThrownBy(() -> exec(c, "DELETE FROM parent_comp WHERE k1=11 AND k2=22"))
                    .isInstanceOf(SQLException.class);

            // sem resíduos
            assertThat(tableExists(c, "__tmp_child_comp")).isFalse();
            assertThat(tableExists(c, "__bak_child_comp")).isFalse();
        }
    }

    @Test
    void normaliza_nomes_de_tabelas_referenciadas_camelcase_e_caseInsensitive() throws Exception {
        SqliteTableRebuilder rb = new SqliteTableRebuilder(dataSource);

        // Queremos adicionar FK em uses_form.dev_id -> "FormDeveloper"(id)
        // A tabela real chama "Form_Developer". O normalizador deve ajustar.
        ForeignKeySpec add = new ForeignKeySpec(
                "dev_id", "FormDeveloper", "id", "RESTRICT", "NO ACTION", "SIMPLE"
        );

        rb.rebuildTableApplyingForeignKeyChanges("uses_form", List.of(add), List.of());

        try (Connection c = dataSource.getConnection()) {
            // Tenta violar: inserir uses_form com dev_id inexistente deve falhar na hora de FK ON (após rebuild já está ON)
            assertThatThrownBy(() -> exec(c, "INSERT INTO uses_form(id,dev_id) VALUES (1,999)"))
                    .isInstanceOf(SQLException.class);

            // insere dev válido e depois usa
            exec(c, "INSERT INTO \"Form_Developer\"(nick) VALUES ('john')");
            long devId = queryLong(c, "SELECT MAX(id) FROM \"Form_Developer\"");
            exec(c, "INSERT INTO uses_form(id,dev_id) VALUES (2," + devId + ")");

            assertThat(countForeignKeyViolations(c)).isZero();
        }
    }

    @Test
    void erro_quando_tabela_referenciada_nao_existe_traz_dica() throws Exception {
        SqliteTableRebuilder rb = new SqliteTableRebuilder(dataSource);

        ForeignKeySpec add = new ForeignKeySpec(
                "dev_id", "TableThatDoesNotExist", "id", "RESTRICT", "NO ACTION", "SIMPLE"
        );

        assertThatThrownBy(() ->
                rb.rebuildTableApplyingForeignKeyChanges("uses_form", List.of(add), List.of())
        )
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Tabela referenciada pela FK não existe")
                .hasMessageContaining("Tabelas existentes:");
    }

    // ========================= Helpers =========================

    private static DataSource simpleDataSource(String jdbcUrl) throws Exception {
        Class.forName("org.sqlite.JDBC");
        return new DataSource() {
            @Override public Connection getConnection() throws SQLException {
                SQLiteConfig cfg = new SQLiteConfig();
                cfg.enforceForeignKeys(true);
                return cfg.createConnection(jdbcUrl);
            }
            @Override public Connection getConnection(String username, String password) throws SQLException { return getConnection(); }
            @Override public <T> T unwrap(Class<T> iface) { throw new UnsupportedOperationException(); }
            @Override public boolean isWrapperFor(Class<?> iface) { return false; }
            @Override public java.io.PrintWriter getLogWriter() { return null; }
            @Override public void setLogWriter(java.io.PrintWriter out) { }
            @Override public void setLoginTimeout(int seconds) { }
            @Override public int getLoginTimeout() { return 0; }
            @Override public java.util.logging.Logger getParentLogger() { return java.util.logging.Logger.getGlobal(); }
        };
    }

    private ForeignKeySpec readSingleFkSpec(String table) throws Exception {
        try (Connection c = dataSource.getConnection()) {
            String sql = "PRAGMA foreign_key_list(" + quote(table) + ")";
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                class Row { int id; String table; String from; String to; String onUpd; String onDel; String match; }
                List<Row> rows = new ArrayList<>();
                while (rs.next()) {
                    Row r = new Row();
                    r.id    = rs.getInt("id");
                    r.table = rs.getString("table");
                    r.from  = rs.getString("from");
                    r.to    = rs.getString("to");
                    r.onUpd = rs.getString("on_update");
                    r.onDel = rs.getString("on_delete");
                    r.match = rs.getString("match");
                    rows.add(r);
                }
                assertThat(rows).isNotEmpty();
                int id = rows.get(0).id;
                String refTable = rows.get(0).table;
                String onUpd = rows.get(0).onUpd;
                String onDel = rows.get(0).onDel;
                String match = rows.get(0).match;
                String baseCsv = joinCsv(rows, r -> r.from);
                String refCsv  = joinCsv(rows, r -> r.to);
                return new ForeignKeySpec(baseCsv, refTable, refCsv, onDel, onUpd, match);
            }
        }
    }

    private static <T> String joinCsv(List<T> rows, java.util.function.Function<T, String> getter) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append(getter.apply(rows.get(i)));
        }
        return sb.toString();
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) { st.execute(sql); }
    }

    private static long queryLong(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    private static String queryString(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private static String readSqlMaster(Connection c, String type, String name) throws SQLException {
        String sql = "SELECT sql FROM sqlite_master WHERE type=? AND name=?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, type);
            ps.setString(2, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    private static boolean tableExists(Connection c, String name) throws SQLException {
        String sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static long countForeignKeyViolations(Connection c) throws SQLException {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA foreign_key_check")) {
            long n = 0;
            while (rs.next()) n++;
            return n;
        }
    }

    private static int getForeignKeysPragma(Connection c) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("PRAGMA foreign_keys")) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }
    private static String normSql(String s) {
        if (s == null) return "";
        return s
                .replace("\"", "")       // remove aspas de identificadores
                .replace("\r", " ")
                .replace("\n", " ")
                .replaceAll("\\s+", " ") // colapsa espaços
                .trim()
                .toLowerCase();
    }

    private static String quote(String id) { return "\"" + id.replace("\"", "\"\"") + "\""; }
}
