package liquibase.ext.db2i.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.SetColumnRemarksGenerator;
import liquibase.statement.core.SetColumnRemarksStatement;

public class SetColumnRemarksGeneratorDB2i extends SetColumnRemarksGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(SetColumnRemarksStatement statement, Database database) {
        return database instanceof DB2iDatabase;
    }

    @Override
    public Sql[] generateSql(SetColumnRemarksStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new Sql[] {
                new UnparsedSql("LABEL ON " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()) + " ("
                        + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getColumnName())
                        + " TEXT IS '" + database.escapeStringForDatabase(statement.getRemarks()) + "')", getAffectedColumn(statement)),
                new UnparsedSql("LABEL ON COLUMN " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()) + "."
                        + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getColumnName())
                        + " IS '" + database.escapeStringForDatabase(statement.getRemarks()) + "'", getAffectedColumn(statement)) };
    }
}
