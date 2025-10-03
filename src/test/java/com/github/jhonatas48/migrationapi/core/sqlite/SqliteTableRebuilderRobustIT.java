package com.github.jhonatas48.migrationapi.core.sqlite;

import com.github.jhonatas48.migrationapi.core.sqlite.SqliteTableRebuilder.ForeignKeySpec;
import org.junit.jupiter.api.*;
import org.sqlite.SQLiteConfig;

import javax.sql.DataSource;
import java.io.File;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.*;

/**
 * Suíte robusta para validar fluxos "cabeludos" do SqliteTableRebuilder.
 */
class SqliteTableRebuilderRobustIT {

    private static final String DB_PATH = "./target/robust.sqlite";
    private DataSource dataSource;

    @BeforeEach
    void setup() throws Exception {
        File f = new File(DB_PATH);
        if (f.exists()) assertThat(f.delete()).isTrue();

        this.dataSource = simpleDataSource("jdbc:sqlite:" + DB_PATH);

        try (Connection c = dataSource.getConnection()) {
            exec(c, "PRAGMA foreign_keys=ON");

            // ---------- Schema base com índices "difíceis" e triggers ----------
            exec(c, """
                CREATE TABLE "weird table"(
                  "Id" INTEGER PRIMARY KEY AUTOINCREMENT,
                  "Select" TEXT NOT NULL DEFAULT 'X', -- coluna com palavra reservada
                  "note" TEXT,
                  CONSTRAINT uq_sel UNIQUE("Select")
                )
                """);

            // Índice com expressão + collation + parcial
            exec(c, """
                CREATE INDEX idx_expr_note
                ON "weird table"(lower("note") COLLATE NOCASE)
                WHERE "note" IS NOT NULL AND "note" <> ''
                """);

            // Índice único "normal"
            exec(c, """ 
                CREATE UNIQUE INDEX idx_unique_select
                ON "weird table"("Select")
                """);

            // Triggers diversos
            exec(c, """
                CREATE TRIGGER trg_weird_bi
                BEFORE INSERT ON "weird table"
                FOR EACH ROW
                WHEN NEW."note" IS NULL
                BEGIN
                  SELECT RAISE(ABORT,'note required');
                END
                """);
            exec(c, """
                CREATE TRIGGER trg_weird_ai
                AFTER INSERT ON "weird table"
                BEGIN
                  UPDATE "weird table" SET "note" = COALESCE("note",'ai') WHERE rowid = NEW.rowid;
                END
                """);

            exec(c, "INSERT INTO \"weird table\"(\"Select\",\"note\") VALUES ('A','abc'),('B','DEF')");

            // ---------- Tabelas para FK e resíduos ----------
            exec(c, """
                CREATE TABLE "PARENT_AI"(
                  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
                  "name" TEXT DEFAULT 'n/a'
                )
                """);

            exec(c, """
                CREATE TABLE "child_ai"(
                  "id" INTEGER PRIMARY KEY,
                  "pid" INTEGER NOT NULL,
                  "v" TEXT DEFAULT 'ok',
                  CONSTRAINT fk_child_ai
                    FOREIGN KEY("pid") REFERENCES "PARENT_AI"("id")
                    ON DELETE CASCADE MATCH SIMPLE
                )
                """);

            exec(c, "INSERT INTO PARENT_AI(name) VALUES ('p1'),('p2')");
            exec(c, "INSERT INTO child_ai(id,pid,v) VALUES (1,1,'x'),(2,2,'y')");

            // cria resíduos propositais
            exec(c, "CREATE TABLE IF NOT EXISTS __tmp_child_ai(id INTEGER)");
            exec(c, "CREATE TABLE IF NOT EXISTS __bak_child_ai(id INTEGER)");
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
    void preserva_indices_com_expressao_collation_parcial_e_triggers_multiplos() throws Exception {
        SqliteTableRebuilder rb = new SqliteTableRebuilder(dataSource);

        // Não mudamos FKs desta tabela; exercita rebuild preservando índices+triggers+defaults+AI
        rb.rebuildTableApplyingForeignKeyChanges("weird table", List.of(), List.of());

        try (Connection c = dataSource.getConnection()) {
            // índices preservados
            String idxExpr = readSqlMaster(c, "index", "idx_expr_note");
            String idxUniq = readSqlMaster(c, "index", "idx_unique_select");
            assertThat(normSql(idxExpr)).contains("create index idx_expr_note on weird table(lower(note) collate nocase)");
            assertThat(normSql(idxExpr)).contains("where note is not null and note <> ''");
            assertThat(normSql(idxUniq)).contains("create unique index idx_unique_select on weird table(select)");

            // triggers preservados
            String trg1 = readSqlMaster(c, "trigger", "trg_weird_bi");
            String trg2 = readSqlMaster(c, "trigger", "trg_weird_ai");
            assertThat(trg1).containsIgnoringCase("before insert");
            assertThat(trg1).contains("WHEN NEW.\"note\" IS NULL");
            assertThat(trg2).containsIgnoringCase("after insert");

            // BEFORE INSERT deve abortar se note for NULL (prova de preservação)
            assertThatThrownBy(() ->
                    exec(c, "INSERT INTO \"weird table\"(\"Select\") VALUES ('C')")
            ).isInstanceOf(SQLException.class)
                    .hasMessageContaining("note required");

            // Inserção válida (note não-nulo) e AUTOINCREMENT preservado
            exec(c, "INSERT INTO \"weird table\"(\"Select\",\"note\") VALUES ('C','ok')");
            long idMax = queryLong(c, "SELECT MAX(\"Id\") FROM \"weird table\"");
            assertThat(idMax).isGreaterThan(2L);

            // valor permanece 'ok' (AFTER apenas preenche quando NULL)
            String lastNote = queryString(c, "SELECT \"note\" FROM \"weird table\" ORDER BY rowid DESC LIMIT 1");
            assertThat(lastNote).isEqualTo("ok");
        }
    }

    @Test
    void limpa_residuos_e_mantem_integridade_fk_apos_troca_da_regra() throws Exception {
        SqliteTableRebuilder rb = new SqliteTableRebuilder(dataSource);

        ForeignKeySpec fkAtual = readSingleFkSpec("child_ai");
        // SQLite pode reportar MATCH como NONE
        assertThat(oneOfSimpleOrNone(fkAtual.getMatch())).isTrue();

        // troca CASCADE -> RESTRICT
        rb.rebuildTableApplyingForeignKeyChanges(
                "child_ai",
                List.of(new ForeignKeySpec(
                        fkAtual.getBaseColumnsCsv(), fkAtual.getReferencedTable(), fkAtual.getReferencedColumnsCsv(),
                        "RESTRICT", fkAtual.getOnUpdate(), fkAtual.getMatch()
                )),
                List.of(fkAtual)
        );

        try (Connection c = dataSource.getConnection()) {
            // resíduos limpos
            assertThat(tableExists(c, "__tmp_child_ai")).isFalse();
            assertThat(tableExists(c, "__bak_child_ai")).isFalse();

            // integridade ok
            assertThat(countForeignKeyViolations(c)).isZero();

            // RESTRICT em ação
            assertThatThrownBy(() -> exec(c, "DELETE FROM PARENT_AI WHERE id=1"))
                    .isInstanceOf(SQLException.class);

            // foreign_keys ON
            assertThat(getForeignKeysPragma(c)).isEqualTo(1);
        }
    }

    @Test
    void rollback_seguro_e_fk_reativada_quando_falha_ao_recriar_indices() throws Exception {
        // Proxy de DataSource -> Connection -> Statement que falha ao recriar 'idx_expr_note'
        DataSource failing = failingDataSource(dataSource, sql ->
                sql != null && normSql(sql).startsWith("create index idx_expr_note"));

        SqliteTableRebuilder rb = new SqliteTableRebuilder(failing);

        // Força um rebuild na tabela que vai tentar recriar o índice de expressão e falhar
        assertThatThrownBy(() -> rb.rebuildTableApplyingForeignKeyChanges("weird table", List.of(), List.of()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Falha forçada para teste")
                .hasMessageContaining("idx_expr_note");

        // Mesmo com falha, garantindo que PRAGMA foreign_keys esteja ON e dados intactos
        try (Connection c = dataSource.getConnection()) {
            assertThat(getForeignKeysPragma(c)).isEqualTo(1);

            // tabela continua íntegra (>= 2 linhas)
            long cnt = queryLong(c, "SELECT COUNT(*) FROM \"weird table\"");
            assertThat(cnt).isGreaterThanOrEqualTo(2L);
        }
    }

    @Test
    void ordem_de_colunas_e_valores_preservados_na_copia() throws Exception {
        // Monta uma tabela com colunas em ordem “estranha” para garantir SELECT/INSERT na mesma ordem
        try (Connection c = dataSource.getConnection()) {
            exec(c, """
                CREATE TABLE "order_test"(
                  "b" TEXT DEFAULT 'B',
                  "a" INTEGER NOT NULL,
                  "c" TEXT
                )
                """);
            exec(c, "INSERT INTO \"order_test\"(a,c) VALUES (42,'C1'),(7,'C2')");
        }

        SqliteTableRebuilder rb = new SqliteTableRebuilder(dataSource);
        rb.rebuildTableApplyingForeignKeyChanges("order_test", List.of(), List.of());

        try (Connection c = dataSource.getConnection()) {
            // valores 1:1 preservados
            List<String> rows = new ArrayList<>();
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT a||':'||b||':'||c FROM \"order_test\" ORDER BY a")) {
                while (rs.next()) rows.add(rs.getString(1));
            }
            assertThat(rows).containsExactly("7:B:C2", "42:B:C1");
        }
    }

    @Test
    void autoincrement_detectado_apenas_em_integer_pk_autoincrement() throws Exception {
        // Tabela com PK numérica mas NÃO INTEGER e sem AUTOINCREMENT -> não deve ser promovida
        try (Connection c = dataSource.getConnection()) {
            exec(c, """
                CREATE TABLE "pk_bigint"(
                  "id" BIGINT PRIMARY KEY, -- não é INTEGER
                  "v"  TEXT
                )
                """);
        }

        SqliteTableRebuilder rb = new SqliteTableRebuilder(dataSource);
        rb.rebuildTableApplyingForeignKeyChanges("pk_bigint", List.of(), List.of());

        try (Connection c = dataSource.getConnection()) {
            String ddl = readCreateFromMaster(c, "pk_bigint");
            String norm = normSql(ddl);
            // Deve permanecer sem AUTOINCREMENT
            assertThat(norm).doesNotContain("autoincrement");
            assertThat(norm).contains("primary key"); // ainda PK
        }
    }

    // ====================================================== helpers ======================================================

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

    /** DataSource que injeta uma falha quando algum Statement.execute(sql) casar com o predicado. */
    private static DataSource failingDataSource(DataSource delegate, java.util.function.Predicate<String> failWhenSqlMatches) {
        return (DataSource) Proxy.newProxyInstance(
                delegate.getClass().getClassLoader(),
                new Class[]{DataSource.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getConnection") && (args == null || args.length == 0)) {
                        Connection c = delegate.getConnection();
                        return connectionProxy(c, failWhenSqlMatches);
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getTargetException();
                    }
                }
        );
    }

    private static Connection connectionProxy(Connection c, java.util.function.Predicate<String> pred) {
        return (Connection) Proxy.newProxyInstance(
                c.getClass().getClassLoader(),
                new Class[]{Connection.class},
                (proxy, method, args) -> {
                    if ((method.getName().equals("createStatement") && (args == null || args.length == 0))
                            || method.getName().equals("createStatement") ) {
                        Statement st = (Statement) method.invoke(c, args);
                        return statementProxy(st, pred);
                    }
                    if (method.getName().equals("prepareStatement")) {
                        return method.invoke(c, args); // não mexemos em PS
                    }
                    try {
                        return method.invoke(c, args);
                    } catch (InvocationTargetException e) {
                        throw e.getTargetException();
                    }
                }
        );
    }

    private static Statement statementProxy(Statement st, java.util.function.Predicate<String> pred) {
        return (Statement) Proxy.newProxyInstance(
                st.getClass().getClassLoader(),
                new Class[]{Statement.class},
                (proxy, method, args) -> {
                    if ((method.getName().equals("execute") || method.getName().equals("executeUpdate")
                            || method.getName().equals("executeLargeUpdate"))
                            && args != null && args.length >= 1 && args[0] instanceof String sql) {
                        if (pred.test(sql)) {
                            throw new RuntimeException("Falha forçada para teste durante recriação de índice/trigger: " + sql);
                        }
                    }
                    try {
                        return method.invoke(st, args);
                    } catch (InvocationTargetException e) {
                        throw e.getTargetException();
                    }
                }
        );
    }

    private static boolean oneOfSimpleOrNone(String m) {
        String s = (m == null || m.isBlank()) ? "NONE" : m.toUpperCase(Locale.ROOT);
        return s.equals("SIMPLE") || s.equals("NONE");
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

    private static boolean tableExists(Connection c, String name) throws SQLException {
        String sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }

    private static String readCreateFromMaster(Connection c, String tableName) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?")) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : "";
            }
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
        return s.replace("\"","")
                .replace("\r", " ")
                .replace("\n", " ")
                .replaceAll("\\s+"," ")
                .trim()
                .toLowerCase(Locale.ROOT);
    }

    private static String quote(String id) {
        return "\"" + id.replace("\"", "\"\"") + "\"";
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
}
