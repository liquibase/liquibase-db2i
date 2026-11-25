package liquibase.ext.db2i.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.ReorganizeTableGeneratorDB2;
import liquibase.statement.core.ReorganizeTableStatement;

/**
 * Generates no REORG on DB2 for IBM i.
 * <p>
 * liquibase-core adds {@code CALL SYSPROC.ADMIN_CMD('REORG TABLE ...')} after some changes (e.g. addColumn) for DB2
 * from version 9 on. {@code ADMIN_CMD} does not exist on IBM i, and IBM i needs no reorganization after
 * {@code ALTER TABLE}. With a connection IBM i reports version 7 and core generates nothing anyway, but without a
 * connection ({@code update-sql}) core assumes a high version and generates the CALL.
 */
public class ReorganizeTableGeneratorDB2i extends ReorganizeTableGeneratorDB2 {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE + 1;
    }

    @Override
    public boolean supports(ReorganizeTableStatement statement, Database database) {
        return database instanceof DB2iDatabase;
    }

    @Override
    public Sql[] generateSql(ReorganizeTableStatement statement, Database database,
                             SqlGeneratorChain sqlGeneratorChain) {
        return new Sql[0];
    }
}
