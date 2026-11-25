package liquibase.ext.db2i.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.SetColumnRemarksGenerator;
import liquibase.statement.core.SetColumnRemarksStatement;

import static liquibase.ext.db2i.sqlgenerator.RemarksUtilDB2i.MAX_HEADING_LENGTH;
import static liquibase.ext.db2i.sqlgenerator.RemarksUtilDB2i.MAX_TEXT_LENGTH;
import static liquibase.ext.db2i.sqlgenerator.RemarksUtilDB2i.truncate;

/**
 * Writes column remarks as column text and column heading via {@code LABEL ON}, truncated to the IBM i limits
 * (text 50, heading 60).
 */
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
        String table = database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(),
                statement.getTableName());
        String column = database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(),
                statement.getTableName(), statement.getColumnName());
        String text = database.escapeStringForDatabase(truncate(statement.getRemarks(), MAX_TEXT_LENGTH));
        String heading = database.escapeStringForDatabase(truncate(statement.getRemarks(), MAX_HEADING_LENGTH));
        return new Sql[] {
                new UnparsedSql("LABEL ON " + table + " (" + column + " TEXT IS '" + text + "')",
                        getAffectedColumn(statement)),
                new UnparsedSql("LABEL ON COLUMN " + table + "." + column + " IS '" + heading + "'",
                        getAffectedColumn(statement)) };
    }
}
