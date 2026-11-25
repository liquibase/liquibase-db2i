package liquibase.ext.db2i.database;

import java.util.Locale;

import liquibase.CatalogAndSchema;
import liquibase.Scope;
import liquibase.database.DatabaseConnection;
import liquibase.database.core.DB2Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.statement.core.GetViewDefinitionStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.util.StringUtil;

public class DB2iDatabase extends DB2Database {

    /** Prefix that tells liquibase-core's ViewSnapshotGenerator that the definition is a complete CREATE VIEW. */
    static final String FULL_DEFINITION_PREFIX = "FULL_DEFINITION: ";

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
     * Reads the view definition from {@code SYSIBM.VIEWS} like liquibase-core, but marks it as full definition only
     * if it really is a complete {@code CREATE VIEW} statement. On IBM i the catalog contains only the query
     * (SELECT part), so liquibase-core's unconditional {@code FULL_DEFINITION:} prefix leads to changelogs with
     * {@code fullDefinition="true"} and a plain SELECT, which do not create the view.
     * <p>
     * IBM i stores no definition (null) if it is longer than the catalog column; liquibase-core then writes
     * {@code [CANNOT READ VIEW DEFINITION]} into the snapshot.
     */
    @Override
    public String getViewDefinition(CatalogAndSchema schema, final String viewName) throws DatabaseException {
        schema = schema.customize(this);
        String definition = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", this)
                .queryForObject(new GetViewDefinitionStatement(schema.getCatalogName(), schema.getSchemaName(),
                        viewName), String.class);
        return markFullDefinition(definition, schema.getSchemaName() + "." + viewName);
    }

    /**
     * Returns the trimmed definition, prefixed with {@code FULL_DEFINITION: } only if it starts with {@code CREATE}.
     * Returns null (and logs a warning) if there is no definition.
     */
    static String markFullDefinition(String definition, String viewName) {
        String trimmed = StringUtil.trimToNull(definition);
        if (trimmed == null) {
            Scope.getCurrentScope().getLog(DB2iDatabase.class).warning("No view definition available for " + viewName
                    + ". IBM i stores no definition in SYSIBM.VIEWS if it is too long.");
            return null;
        }
        if (trimmed.toUpperCase(Locale.ROOT).startsWith("CREATE")) {
            return FULL_DEFINITION_PREFIX + trimmed;
        }
        return trimmed;
    }
}
