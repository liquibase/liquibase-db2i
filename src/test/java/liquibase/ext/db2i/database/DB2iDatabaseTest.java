package liquibase.ext.db2i.database;

import liquibase.datatype.DataTypeFactory;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class DB2iDatabaseTest {

    @Test
    public void getShortName() {
        assertEquals("db2i", new DB2iDatabase().getShortName());
    }

    @Test
    public void timestampWith12FractionalDigitsIsAllowed() {
        // with a connection IBM i reports major version 7
        DB2iDatabase database = new DB2iDatabase() {
            @Override
            public int getDatabaseMajorVersion() {
                return 7;
            }
        };
        assertEquals(12, database.getMaxFractionalDigitsForTimestamp());
        assertEquals("TIMESTAMP(12)", DataTypeFactory.getInstance().fromDescription("TIMESTAMP(12)", database)
                .toDatabaseDataType(database).toSql().toUpperCase());
    }
}
