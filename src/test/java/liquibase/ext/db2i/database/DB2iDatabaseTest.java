package liquibase.ext.db2i.database;

import liquibase.database.jvm.JdbcConnection;
import liquibase.license.TargetUniquenessAttributes;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

public class DB2iDatabaseTest {

    @Test
    public void getShortName() {
        assertEquals("db2i", new DB2iDatabase().getShortName());
    }

    // ---- Target Uniqueness ----

    @Test
    public void getTargetUniquenessAttributes_returnsDatabaseNameAsDb2iAndSchemaAsNull() throws Exception {
        DB2iDatabase db = new DB2iDatabase();
        db.setDefaultCatalogName("MYLIB");
        db.setConnection(mockDb2iConnection("jdbc:as400://host/MYLIB", "MYLIB"));

        TargetUniquenessAttributes attrs = db.getTargetUniquenessAttributes();

        assertEquals("jdbc:as400://host/MYLIB", attrs.getSanitizedUrl());
        assertEquals("DB2i", attrs.getDatabaseName());
        assertEquals("MYLIB", attrs.getCatalog());
        assertNull(attrs.getSchema());
    }

    /**
     * Core licensing assertion for DB2 iSeries: the license unit is the library (collection),
     * stored in Liquibase's catalog slot via SELECT CURRENT SCHEMA. Two connections to the same
     * iSeries system targeting different libraries must count as two distinct targets.
     */
    @Test
    public void getTargetUniquenessAttributes_db2iDifferentLibraries_produceDifferentTargetIds() throws Exception {
        DB2iDatabase db1 = new DB2iDatabase();
        db1.setDefaultCatalogName("MYLIB");
        db1.setConnection(mockDb2iConnection("jdbc:as400://host/MYLIB", "MYLIB"));
        DB2iDatabase db2 = new DB2iDatabase();
        db2.setDefaultCatalogName("OTHERLIB");
        db2.setConnection(mockDb2iConnection("jdbc:as400://host/OTHERLIB", "OTHERLIB"));

        TargetUniquenessAttributes attrs1 = db1.getTargetUniquenessAttributes();
        TargetUniquenessAttributes attrs2 = db2.getTargetUniquenessAttributes();

        assertNotEquals(attrs1.getTargetId(), attrs2.getTargetId());
    }

    /**
     * DB2 iSeries has {@code supportsSchemas()=true}, so the base implementation would include
     * {@code connection.getSchema()} in the canonical string. That would cause two connections
     * to the same library but with different current schemas to produce different target IDs —
     * violating the Target Uniqueness Definitions. Our {@code resolveSchema()} override nullifies
     * the schema so these collapse correctly.
     */
    @Test
    public void getTargetUniquenessAttributes_db2iSameLibraryDifferentUnderlyingSchema_produceSameTargetId() throws Exception {
        DB2iDatabase db1 = new DB2iDatabase();
        db1.setDefaultCatalogName("MYLIB");
        db1.setConnection(mockDb2iConnection("jdbc:as400://host/MYLIB", "MYLIB"));
        DB2iDatabase db2 = new DB2iDatabase();
        db2.setDefaultCatalogName("MYLIB");
        db2.setConnection(mockDb2iConnection("jdbc:as400://host/MYLIB", "OTHERSCHEMA"));

        TargetUniquenessAttributes attrs1 = db1.getTargetUniquenessAttributes();
        TargetUniquenessAttributes attrs2 = db2.getTargetUniquenessAttributes();

        assertEquals(attrs1.getTargetId(), attrs2.getTargetId());
    }

    /**
     * Mock a DB2 iSeries JDBC connection. Detection in {@link DB2iDatabase#isCorrectDatabaseImplementation}
     * relies on product name starting with {@code "DB2 UDB for AS/400"}. Tests set
     * {@code defaultCatalogName} on the database directly to skip the live
     * {@code SELECT CURRENT SCHEMA} query.
     */
    private JdbcConnection mockDb2iConnection(String url, String schema) throws Exception {
        JdbcConnection jdbcConn = Mockito.mock(JdbcConnection.class);
        Connection underlying = Mockito.mock(Connection.class);
        DatabaseMetaData metadata = Mockito.mock(DatabaseMetaData.class);
        Statement statement = Mockito.mock(Statement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);

        when(jdbcConn.getURL()).thenReturn(url);
        when(jdbcConn.getCatalog()).thenReturn(null);
        when(jdbcConn.getAutoCommit()).thenReturn(false);
        when(jdbcConn.getUnderlyingConnection()).thenReturn(underlying);
        when(jdbcConn.getDatabaseProductName()).thenReturn("DB2 UDB for AS/400");
        when(jdbcConn.getDatabaseProductVersion()).thenReturn("07.04.0000 V7R4m0");
        when(jdbcConn.isClosed()).thenReturn(false);
        when(jdbcConn.createStatement()).thenReturn(statement);
        when(statement.executeQuery(Mockito.anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);
        when(underlying.getSchema()).thenReturn(schema);
        when(underlying.getMetaData()).thenReturn(metadata);
        when(metadata.supportsMixedCaseIdentifiers()).thenReturn(false);

        return jdbcConn;
    }
}
