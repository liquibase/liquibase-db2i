package liquibase.ext.db2i.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.SetTableRemarksGenerator;
import liquibase.statement.core.SetTableRemarksStatement;

import static liquibase.ext.db2i.sqlgenerator.RemarksUtilDB2i.MAX_TEXT_LENGTH;
import static liquibase.ext.db2i.sqlgenerator.RemarksUtilDB2i.truncate;

/**
 * Writes table remarks as object text via {@code LABEL ON TABLE}, truncated to the IBM i limit of 50.
 */
public class SetTableRemarksGeneratorDB2i extends SetTableRemarksGenerator {

    @Override
    public boolean supports(SetTableRemarksStatement statement, Database database) {
        return database instanceof DB2iDatabase;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public Sql[] generateSql(SetTableRemarksStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String table = database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(),
                statement.getTableName());
        String remarks = database.escapeStringForDatabase(truncate(statement.getRemarks(), MAX_TEXT_LENGTH));
        String sql = "LABEL ON TABLE " + table + " IS '" + remarks + "'";

        return new Sql[]{new UnparsedSql(sql, getAffectedTable(statement))};
    }
}
