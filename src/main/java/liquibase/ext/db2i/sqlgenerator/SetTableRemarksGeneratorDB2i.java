package liquibase.ext.db2i.sqlgenerator;

import liquibase.database.Database;
import liquibase.database.core.MySQLDatabase;
import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.SetTableRemarksGenerator;
import liquibase.statement.core.SetTableRemarksStatement;

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
        String sql = "LABEL ON TABLE " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName())
                + " IS '"
                + database.escapeStringForDatabase(statement.getRemarks()) + "'";

        return new Sql[]{new UnparsedSql(sql, getAffectedTable(statement))};
    }
}
