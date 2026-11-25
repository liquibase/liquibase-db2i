package liquibase.ext.db2i.sqlgenerator;

import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sql.Sql;
import liquibase.statement.core.InsertOrUpdateStatement;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class InsertOrUpdateGeneratorDB2iTest {

    private static Sql[] generate(InsertOrUpdateStatement stmt) {
        return new InsertOrUpdateGeneratorDB2i().generateSql(stmt, new DB2iDatabase(), null);
    }

    /** SQL with whitespace collapsed to single blanks, so assertions do not depend on formatting. */
    private static String sql(InsertOrUpdateStatement stmt) {
        Sql[] sql = generate(stmt);
        assertEquals(1, sql.length);
        return sql[0].toSql().replaceAll("\\s+", " ").replace("( ", "(").replace(" )", ")").trim();
    }

    private static InsertOrUpdateStatement idNameActive() {
        InsertOrUpdateStatement stmt = new InsertOrUpdateStatement(null, null, "T", "ID");
        stmt.addColumnValue("ID", 1);
        stmt.addColumnValue("NAME", "Foo");
        stmt.addColumnValue("ACTIVE", "*YES");
        return stmt;
    }

    @Test
    public void defaultUpdatesAllNonPrimaryKeyColumns() {
        assertEquals("MERGE INTO T AS DST USING (VALUES (1, 'Foo', '*YES')) AS SRC(ID,NAME,ACTIVE) ON DST.ID = SRC.ID"
                        + " WHEN NOT MATCHED THEN INSERT(ID, NAME, ACTIVE) VALUES (SRC.ID, SRC.NAME, SRC.ACTIVE)"
                        + " WHEN MATCHED THEN UPDATE SET NAME = 'Foo', ACTIVE = '*YES'",
                sql(idNameActive()));
    }

    @Test
    public void allowUpdateFalseColumnIsInsertedButNotUpdated() {
        InsertOrUpdateStatement stmt = idNameActive();
        stmt.setAllowColumnUpdate("ACTIVE", false);
        String sql = sql(stmt);
        assertTrue(sql, sql.contains(
                "WHEN NOT MATCHED THEN INSERT(ID, NAME, ACTIVE) VALUES (SRC.ID, SRC.NAME, SRC.ACTIVE)"));
        assertTrue(sql, sql.endsWith("WHEN MATCHED THEN UPDATE SET NAME = 'Foo'"));
    }

    @Test
    public void noUpdatableColumnsOmitsWhenMatched() {
        InsertOrUpdateStatement stmt = idNameActive();
        stmt.setAllowColumnUpdate("NAME", false);
        stmt.setAllowColumnUpdate("ACTIVE", false);
        String sql = sql(stmt);
        assertTrue(sql, sql.contains("WHEN NOT MATCHED THEN INSERT"));
        assertFalse(sql, sql.contains("WHEN MATCHED"));
        assertFalse(sql, sql.contains("UPDATE"));
    }

    @Test
    public void onlyPrimaryKeyColumnsOmitsWhenMatched() {
        InsertOrUpdateStatement stmt = new InsertOrUpdateStatement(null, null, "T", "ID,TYPE");
        stmt.addColumnValue("ID", 1);
        stmt.addColumnValue("TYPE", "A");
        String sql = sql(stmt);
        assertEquals("MERGE INTO T AS DST USING (VALUES (1, 'A')) AS SRC(ID,TYPE)"
                + " ON DST.ID = SRC.ID AND DST.TYPE = SRC.TYPE"
                + " WHEN NOT MATCHED THEN INSERT(ID, TYPE) VALUES (SRC.ID, SRC.TYPE)", sql);
    }

    @Test
    public void onlyUpdateOmitsWhenNotMatched() {
        InsertOrUpdateStatement stmt = idNameActive();
        stmt.setOnlyUpdate(true);
        assertEquals("MERGE INTO T AS DST USING (VALUES (1, 'Foo', '*YES')) AS SRC(ID,NAME,ACTIVE) ON DST.ID = SRC.ID"
                + " WHEN MATCHED THEN UPDATE SET NAME = 'Foo', ACTIVE = '*YES'", sql(stmt));
    }

    @Test
    public void onlyUpdateWithoutUpdatableColumnsGeneratesNothing() {
        InsertOrUpdateStatement stmt = idNameActive();
        stmt.setOnlyUpdate(true);
        stmt.setAllowColumnUpdate("NAME", false);
        stmt.setAllowColumnUpdate("ACTIVE", false);
        assertEquals(0, generate(stmt).length);
    }

    @Test
    public void primaryKeyNamesAreTrimmed() {
        InsertOrUpdateStatement stmt = new InsertOrUpdateStatement(null, null, "T", "ID, TYPE");
        stmt.addColumnValue("ID", 1);
        stmt.addColumnValue("TYPE", "A");
        stmt.addColumnValue("NAME", "Foo");
        String sql = sql(stmt);
        assertTrue(sql, sql.contains(" ON DST.ID = SRC.ID AND DST.TYPE = SRC.TYPE WHEN NOT MATCHED"));
        assertTrue(sql, sql.endsWith("WHEN MATCHED THEN UPDATE SET NAME = 'Foo'"));
    }

    /**
     * NULL handling is unchanged compared to the previous implementation: NULL columns are missing in SRC, INSERT
     * uses NULL, UPDATE SET uses NULL. Only the primary key column is no longer part of UPDATE SET.
     */
    @Test
    public void nullValueHandlingUnchanged() {
        InsertOrUpdateStatement stmt = new InsertOrUpdateStatement(null, null, "T", "ID");
        stmt.addColumnValue("ID", 1);
        stmt.addColumnValue("NAME", "NULL");
        stmt.addColumnValue("ACTIVE", "*YES");
        // previous implementation: ... WHEN MATCHED THEN UPDATE SET ID = 1, NAME = NULL, ACTIVE = '*YES'
        assertEquals("MERGE INTO T AS DST USING (VALUES (1, '*YES')) AS SRC(ID,ACTIVE) ON DST.ID = SRC.ID"
                        + " WHEN NOT MATCHED THEN INSERT(ID, NAME, ACTIVE) VALUES (SRC.ID, NULL, SRC.ACTIVE)"
                        + " WHEN MATCHED THEN UPDATE SET NAME = NULL, ACTIVE = '*YES'",
                sql(stmt));
    }
}
