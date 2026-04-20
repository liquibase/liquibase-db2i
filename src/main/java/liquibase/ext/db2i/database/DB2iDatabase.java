package liquibase.ext.db2i.database;

import liquibase.Scope;
import liquibase.database.DatabaseConnection;
import liquibase.database.core.DB2Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.statement.core.RawSqlStatement;

public class DB2iDatabase extends DB2Database {

    @Override
    public int getPriority() {
        return super.getPriority()+5;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return conn.getDatabaseProductName().startsWith("DB2 UDB for AS/400");
    }

    @Override
    public String getDefaultDriver(String url) {
        if (url.startsWith("jdbc:as400")) {
            return "com.ibm.as400.access.AS400JDBCDriver";
        }
        return null;
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return "DB2i";
    }

    @Override
    public String getShortName() {
        return "db2i";
    }

    @Override
    public boolean supportsSchemas() {
        return true;
    }

    @Override
	public boolean supportsBooleanDataType() {
        if (getConnection() == null)
            return false;
        try {
            final Integer countBooleanType = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", this).queryForObject(
                    new RawSqlStatement("select count(*) from sysibm.sqltypeinfo where type_name = 'BOOLEAN'"),
                    Integer.class);
            return countBooleanType == 1;
        } catch (final Exception e) {
            Scope.getCurrentScope().getLog(getClass()).info("Error checking for BOOLEAN type", e);
        }
        return false;
    }

    /**
     * DB2 iSeries is the only DB2 variant with {@code supportsSchemas()=true}. Without this override,
     * the base implementation returns the library/collection name as both {@code catalog} AND
     * {@code schema} in the target uniqueness attributes (because in DB2 the catalog slot holds the
     * authorization ID, which on iSeries equals the current library).
     */
    @Override
    protected String resolveSchema(JdbcConnection jdbcConn) {
        return null;
    }
}
