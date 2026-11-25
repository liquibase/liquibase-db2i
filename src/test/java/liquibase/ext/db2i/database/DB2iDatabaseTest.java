package liquibase.ext.db2i.database;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class DB2iDatabaseTest {

    @Test
    public void getShortName() {
        assertEquals("db2i", new DB2iDatabase().getShortName());
    }

    @Test
    public void viewQueryIsNotMarkedAsFullDefinition() {
        assertEquals("SELECT A FROM T", DB2iDatabase.markFullDefinition("  SELECT A FROM T  ", "S.V"));
    }

    @Test
    public void createViewStatementIsMarkedAsFullDefinition() {
        assertEquals("FULL_DEFINITION: CREATE VIEW V AS SELECT A FROM T",
                DB2iDatabase.markFullDefinition("CREATE VIEW V AS SELECT A FROM T", "S.V"));
        assertEquals("FULL_DEFINITION: create or replace view V as select A from T",
                DB2iDatabase.markFullDefinition("create or replace view V as select A from T", "S.V"));
    }

    @Test
    public void missingViewDefinitionStaysNull() {
        assertEquals(null, DB2iDatabase.markFullDefinition(null, "S.V"));
        assertEquals(null, DB2iDatabase.markFullDefinition("  ", "S.V"));
    }
}
