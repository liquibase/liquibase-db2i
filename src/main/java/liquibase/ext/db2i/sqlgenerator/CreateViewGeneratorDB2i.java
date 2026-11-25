package liquibase.ext.db2i.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateViewGenerator;
import liquibase.statement.core.CreateViewStatement;

/**
 * Allows {@code replaceIfExists} on DB2 for IBM i. liquibase-core rejects it for DB2 below version 10.5, a limit
 * that applies to DB2 LUW; IBM i reports version 7 and supports {@code CREATE OR REPLACE VIEW} since 7.1.
 * <p>
 * Note: on IBM i, replacing a view drops its LABEL and COMMENT, so view remarks should be set in the same
 * {@code createView}.
 */
public class CreateViewGeneratorDB2i extends CreateViewGenerator {

    @Override
    public boolean supports(CreateViewStatement statement, Database database) {
        return database instanceof DB2iDatabase;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public ValidationErrors validate(CreateViewStatement createViewStatement, Database database,
                                     SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();

        validationErrors.checkRequiredField("viewName", createViewStatement.getViewName());

        return validationErrors;
    }
}
