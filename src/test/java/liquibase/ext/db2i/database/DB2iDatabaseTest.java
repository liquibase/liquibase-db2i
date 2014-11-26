package liquibase.ext.db2i.database;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class DB2iDatabaseTest {

    @Test
    public void getShortName() {
        assertEquals("db2i", new DB2iDatabase().getShortName());
    }
}
