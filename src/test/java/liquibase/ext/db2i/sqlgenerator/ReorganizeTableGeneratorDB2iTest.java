package liquibase.ext.db2i.sqlgenerator;

import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.ReorganizeTableGeneratorDB2;
import liquibase.statement.core.ReorganizeTableStatement;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReorganizeTableGeneratorDB2iTest {

    /** Without a connection, as with update-sql: liquibase-core assumes version 999 and would generate ADMIN_CMD. */
    private final DB2iDatabase offline = new DB2iDatabase();

    private final ReorganizeTableStatement statement = new ReorganizeTableStatement(null, null, "T");

    @Test
    public void db2iGeneratorIsUsed() {
        assertTrue(SqlGeneratorFactory.getInstance().getGenerators(statement, offline).first()
                instanceof ReorganizeTableGeneratorDB2i);
    }

    @Test
    public void coreGeneratorWouldCallAdminCmd() {
        assertTrue(new ReorganizeTableGeneratorDB2().generateSql(statement, offline, null)[0].toSql()
                .startsWith("CALL SYSPROC.ADMIN_CMD"));
    }

    @Test
    public void noReorgIsGeneratedWithoutConnection() {
        assertEquals(0, SqlGeneratorFactory.getInstance().generateSql(statement, offline).length);
    }
}
