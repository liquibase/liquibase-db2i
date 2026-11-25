package liquibase.ext.db2i.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AddColumnGeneratorDefaultClauseBeforeNotNull;
import liquibase.statement.core.AddColumnStatement;

import java.util.List;

/**
 * Allows {@code beforeColumn} for {@code addColumn} on DB2 for IBM i, which supports {@code ALTER TABLE ... ADD ...
 * BEFORE col}. All other validations of liquibase-core stay in place, in particular {@code afterColumn} and
 * {@code position} remain errors because IBM i does not support them.
 * <p>
 * Extends {@link AddColumnGeneratorDefaultClauseBeforeNotNull}, which liquibase-core uses for DB2, so the generated
 * SQL is the same as without this extension (e.g. {@code DEFAULT 5 NOT NULL} without a constraint name, which IBM i
 * would reject).
 * <p>
 * Additionally rejects NOT NULL columns without a default value, which IBM i does not allow for ADD COLUMN.
 */
public class AddColumnGeneratorDB2i extends AddColumnGeneratorDefaultClauseBeforeNotNull {

    @Override
    public boolean supports(AddColumnStatement statement, Database database) {
        return database instanceof DB2iDatabase;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE + 1;
    }

    @Override
    public ValidationErrors validate(AddColumnStatement statement, Database database,
                                     SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = super.validate(statement, database, sqlGeneratorChain);
        validationErrors.getErrorMessages().removeAll(beforeColumnNotAllowedMessage(database));
        if (statement.isMultiple()) {
            for (AddColumnStatement column : statement.getColumns()) {
                validateNotNullHasDefault(column, validationErrors);
            }
        } else {
            validateNotNullHasDefault(statement, validationErrors);
        }
        return validationErrors;
    }

    /**
     * IBM i rejects {@code ALTER TABLE ... ADD col ... NOT NULL} without a DEFAULT with SQL0109, even on an empty
     * table. Identity columns are the exception. Reporting it here stops the update before the first changeset runs
     * instead of failing in the middle of it. Primary key columns are already rejected by liquibase-core.
     */
    private static void validateNotNullHasDefault(AddColumnStatement column, ValidationErrors validationErrors) {
        if (!column.isNullable() && !column.isPrimaryKey() && !column.isAutoIncrement()
                && column.getDefaultValue() == null) {
            validationErrors.addError("Adding NOT NULL column '" + column.getColumnName()
                    + "' requires a default value on db2i (defaultValue or defaultValueComputed)");
        }
    }

    /**
     * The message liquibase-core reports for {@code beforeColumn}, created by the same core method, so it stays in
     * sync if liquibase changes the wording.
     */
    private static List<String> beforeColumnNotAllowedMessage(Database database) {
        ValidationErrors expected = new ValidationErrors();
        expected.checkDisallowedField("addBeforeColumn", "", database);
        return expected.getErrorMessages();
    }
}
