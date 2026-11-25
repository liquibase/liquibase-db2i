package liquibase.ext.db2i.sqlgenerator;

import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.CreateViewGenerator;
import liquibase.statement.core.CreateViewStatement;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CreateViewGeneratorDB2iTest {

    /** IBM i reports major version 7 when connected. */
    private final DB2iDatabase database = new DB2iDatabase() {
        @Override
        public int getDatabaseMajorVersion() {
            return 7;
        }
    };

    private final CreateViewStatement replaceView = new CreateViewStatement(null, null, "V", "SELECT A FROM T", true);

    @Test
    public void coreGeneratorRejectsReplaceIfExistsOnVersion7() {
        assertTrue(new CreateViewGenerator().validate(replaceView, database, null).hasErrors());
    }

    @Test
    public void replaceIfExistsIsAllowedOnIbmI() {
        assertFalse(SqlGeneratorFactory.getInstance().validate(replaceView, database).hasErrors());
        Sql[] sql = SqlGeneratorFactory.getInstance().generateSql(replaceView, database);
        assertEquals(1, sql.length);
        assertEquals("CREATE OR REPLACE VIEW V AS SELECT A FROM T", sql[0].toSql());
    }

    @Test
    public void viewNameIsRequired() {
        CreateViewStatement statement = new CreateViewStatement(null, null, null, "SELECT A FROM T", true);
        assertTrue(SqlGeneratorFactory.getInstance().validate(statement, database).hasErrors());
    }
}
